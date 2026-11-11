// ============================================================================
// controller.cpp - Controller: orchestrates the six engines (spec §14)
//
// The Controller composes E1..E6 without absorbing their responsibilities:
//   E1 ChunkManager     - chunking, SHA-256, chunk metadata
//   E2 TCPClient        - request/response communication with peers
//   E3 Distributor      - round-robin placement + replication
//   E4 FailureDetector  - heartbeat-based failure detection
//   E5 RecoveryManager  - recovery + reconstruction
//   E6 MetadataManager  - metadata + monitoring
//
// CLI:
//   controller sys <start|stop|status>   start / stop / list the 3 peer processes
//   controller fail <A|B|C|random>       terminate one peer (simulated failure)
//   controller demo [file]               full Gate-1 demonstration
//   controller test [T01 ...]            run the Gate-1 tests T01..T07
//   controller chunk <file>
//   controller distribute
//   controller ping
//   controller detect [--all] [--duration-ms N]
//   controller recover [--failed <id>] [--simulate-interrupt]
//   controller reconstruct [original_file]
//   controller status
//   controller reset
//
// The controller finds the project root (the directory holding config.json)
// by itself, so it can be started from any working directory.
// ============================================================================
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iterator>
#include <mutex>
#include <set>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <sstream>
#include <thread>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/coding/adaptive_chunker.hpp"
#include "../engine/coding/reed_solomon.hpp"
#include "../engine/communication/socket_compat.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/db/database_manager.hpp"
#include "../engine/coordinator/coordinator_service.hpp"
#include "../engine/distribution/distributor.hpp"
#include "../engine/failure_detection/failure_detector.hpp"
#include "../engine/metadata/metadata_manager.hpp"
#include "../engine/process/process_manager.hpp"
#include "../engine/recovery/client_download.hpp"
#include "../engine/recovery/recovery_manager.hpp"
#include "../engine/registry/peer_registry.hpp"
#include "../models/peer.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

class Controller {
public:
    Controller() { load_config(); }

    // ---- configuration -------------------------------------------------------
    void load_config() {
        std::ifstream in("config.json");
        if (!in) throw std::runtime_error("config.json not found - run from the project root");
        std::stringstream ss;
        ss << in.rdbuf();
        config_ = json::Json::parse(ss.str());

        chunk_size_ = static_cast<uint64_t>(config_["chunk_size"].as_int());
        hb_interval_ = static_cast<int>(config_["heartbeat_interval_ms"].as_int());
        hb_timeout_ = static_cast<int>(config_["heartbeat_timeout_ms"].as_int());
        hb_threshold_ = static_cast<int>(config_["failure_threshold"].as_int());

        for (const auto& p : config_["peers"].items()) {
            PeerInfo pi;
            pi.id = p["id"].as_string();
            pi.address = p["address"].as_string();
            pi.port = static_cast<uint16_t>(p["port"].as_int());
            peers_.push_back(pi);
        }

        if (config_.contains("coordinator")) {
            coord_bind_address_ = config_["coordinator"]["address"].as_string();
            coord_port_ = static_cast<uint16_t>(config_["coordinator"]["port"].as_int());
        }
    }

    // ---- Gate-2: Coordinator Service & Peer Registry commands -----------------
    int cmd_coordinator(uint16_t port = 0, const std::string& bind_addr = "") {
        if (port == 0) port = coord_port_;
        std::string addr = bind_addr.empty() ? coord_bind_address_ : bind_addr;

        CoordinatorService coordinator(addr, port, hb_interval_, hb_timeout_, hb_threshold_);
        if (!coordinator.start()) return 1;

        std::cout << "[Coordinator] Service listening. Press Ctrl+C to terminate.\n";
        proc::install_shutdown_handler([&coordinator]() {
            coordinator.stop();
        });
        coordinator.run();
        proc::remove_shutdown_handler();
        return 0;
    }

    int cmd_peers(const std::string& coord_addr = "", uint16_t coord_port = 0) {
        std::string addr = coord_addr.empty() ? (coord_bind_address_ == "0.0.0.0" ? "127.0.0.1" : coord_bind_address_) : coord_addr;
        uint16_t port = (coord_port == 0) ? coord_port_ : coord_port;

        TCPClient client(1500, 1500);
        if (!client.connect_to(addr, port)) {
            std::cerr << "controller error: cannot connect to Coordinator at " << addr << ":" << port << "\n";
            return 1;
        }

        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
            resp["type"].as_string() != proto::QUERY_PEERS_ACK) {
            std::cerr << "controller error: failed to query peers from Coordinator\n";
            return 1;
        }

        PeerRegistry reg;
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                std::string msg;
                reg.register_peer(pi, msg);
            }
        }
        std::cout << reg.format_table();
        return 0;
    }

    // ---- E1: chunk ------------------------------------------------------------
    int cmd_chunk(const std::string& file) {
        std::error_code fec;
        const fs::path file_path = fs::u8path(file);  // UTF-8 -> native path
        if (!fs::is_regular_file(file_path, fec)) {
            std::cerr << "input file not found: " << file << "\n";
            return 1;
        }
        const uint64_t size = fs::file_size(file_path);
        const std::string fid = meta_.next_file_id();

        ChunkManager cm(chunk_size_);
        auto chunks = cm.chunk_file(fid, file, staging_dir());

        meta_.init_file_metadata(fid, file_path.filename().u8string(),
                                 size, chunk_size_, chunks);
        // Remember where the original lives, for the final byte-comparison.
        {
            json::Json d = json::Json::object();
            d["file_id"] = fid;
            d["source_path"] = fs::absolute(file_path).u8string();
            meta_.log_event("FILE_CHUNKED", "file split into fixed-size chunks", d);
        }
        // Store source path in metadata for reconstruct's default.
        {
            // cheap approach: separate sidecar, keeps file_metadata.json schema clean
            std::ofstream out("metadata/source_path.txt", std::ios::trunc);
            out << fs::absolute(file_path).u8string();
        }

        std::cout << "[E1] file=" << file << " id=" << fid << " size=" << size
                  << " chunks=" << chunks.size() << " chunk_size=" << chunk_size_
                  << "\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " seq=" << c.sequence
                      << " size=" << c.size << " sha256=" << c.sha256.substr(0, 16)
                      << "...\n";
        return 0;
    }

    // ---- E3: distribute ---------------------------------------------------------
    int cmd_distribute() {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata - run 'chunk' first\n";
            return 1;
        }
        auto chunks = meta_.chunks();
        Distributor dist(peers_);
        std::string err;
        if (!dist.distribute(staging_dir(), chunks, err)) {
            std::cerr << "[E3] distribution failed: " << err << "\n";
            meta_.log_event("DISTRIBUTION_FAILED", err);
            return 1;
        }
        for (const auto& c : chunks) meta_.set_chunk_peers(c.chunk_id, c.peers);

        std::cout << "[E3] round-robin placement, replication factor 2:\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " -> " << c.peers[0] << " + "
                      << c.peers[1] << "\n";
        json::Json d = json::Json::object();
        d["total_chunks"] = static_cast<long long>(chunks.size());
        d["replication_factor"] = 2;
        meta_.log_event("DISTRIBUTION_DONE",
                        "chunks distributed round-robin with replication", d);
        return 0;
    }

    // ---- E2: REGISTER handshake ----------------------------------------------------
    bool ping_peer(const PeerInfo& p, int timeout_ms = 1000) {
        TCPClient client(timeout_ms, timeout_ms);
        if (!client.connect_to(p.address, p.port)) return false;
        json::Json req = json::Json::object();
        req["type"] = proto::REGISTER;
        json::Json resp;
        std::vector<uint8_t> pl;
        return client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
               resp["type"].as_string() == proto::REGISTER_ACK;
    }

    int cmd_ping() {
        bool all_ok = true;
        for (const auto& p : peers_) {
            const bool ok = ping_peer(p);
            std::cout << "  peer " << p.id << " (" << p.address << ":" << p.port
                      << ") " << (ok ? "ONLINE" : "UNREACHABLE") << "\n";
            all_ok &= ok;
        }
        return all_ok ? 0 : 1;
    }

    // ---- E4: heartbeat failure detection --------------------------------------------
    // duration_ms > 0 bounds the monitoring time (used with --all).
    int cmd_detect(bool stop_on_first, long long duration_ms = 0) {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::cout << "[E4] heartbeat monitor: interval=" << hb_interval_
                  << "ms timeout=" << hb_timeout_
                  << "ms threshold=" << hb_threshold_ << " missed\n";

        for (const auto& p : fd.probe_once())
            meta_.set_peer_status(p.id, p.state);

        std::mutex mu;
        std::string failed;
        long long detection_ms = 0;
        std::atomic<bool> finished{false};
        std::thread timer;
        if (duration_ms > 0) {
            timer = std::thread([&]() {
                const auto until = std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(duration_ms);
                while (!finished && std::chrono::steady_clock::now() < until)
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                fd.stop();
            });
        }
        fd.run([&](const PeerInfo& peer, long long ms) {
            {
                std::lock_guard<std::mutex> lk(mu);
                if (failed.empty()) {
                    failed = peer.id;
                    detection_ms = ms;
                }
            }
            meta_.set_peer_status(peer.id, PeerState::OFFLINE);
            json::Json d = json::Json::object();
            d["peer"] = peer.id;
            d["detection_ms"] = ms;
            meta_.log_event("PEER_FAILURE_DETECTED",
                            "heartbeat threshold reached; peer marked OFFLINE", d);
            std::lock_guard<std::mutex> lk(mu);
            std::cout << "[E4] peer " << peer.id << " OFFLINE (detection time "
                      << ms << " ms, measured)\n";
        }, stop_on_first);
        finished = true;
        if (timer.joinable()) timer.join();

        if (!failed.empty()) {
            std::cout << failed << " " << detection_ms << "\n";  // machine-readable
            return 0;
        }
        return 2;  // no failure observed
    }

    // ---- failure simulation: terminate one peer process ------------------------------
    // Accepts a configured peer id (case-insensitive); returns the canonical id.
    bool resolve_peer_id(const std::string& wanted, std::string& id_out) const {
        for (const auto& p : peers_) {
            if (p.id.size() == wanted.size() &&
                std::equal(p.id.begin(), p.id.end(), wanted.begin(),
                           [](char x, char y) {
                               return std::toupper(static_cast<unsigned char>(x)) ==
                                      std::toupper(static_cast<unsigned char>(y));
                           })) {
                id_out = p.id;
                return true;
            }
        }
        return false;
    }

    bool kill_peer_process(const std::string& target, long& pid_out) {
        const long pid = proc::read_pid_file(proc::pid_file_for(target));
        if (pid <= 0) {
            std::cerr << "no pid file for peer " << target
                      << " (is the system running? try: controller sys start)\n";
            return false;
        }
        if (!proc::peer_running(pid)) {
            std::cerr << "peer " << target << " (pid " << pid
                      << ") is not running\n";
            return false;
        }
        std::string err;
        if (!proc::terminate_peer(pid, err)) {
            std::cerr << "failed to terminate peer " << target << ": " << err << "\n";
            return false;
        }
        pid_out = pid;
        return true;
    }

    int cmd_fail(const std::string& which) {
        std::string target;
        if (which == "random") {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            target = peers_[pick(gen)].id;
        } else if (!resolve_peer_id(which, target)) {
            std::cerr << "unknown peer '" << which << "' (expected";
            for (const auto& p : peers_) std::cerr << " " << p.id;
            std::cerr << " or random)\n";
            return 1;
        }
        long pid = 0;
        if (!kill_peer_process(target, pid)) return 1;
        json::Json d = json::Json::object();
        d["peer"] = target;
        d["pid"] = pid;
        meta_.log_event("PEER_KILLED", "peer process terminated (simulated failure)", d);
        std::cout << "[demo] peer " << target << " (pid " << pid
                  << ") terminated - failure simulated\n";
        std::cout << target << "\n";
        return 0;
    }

    // ---- system process control (native; replaces the run_system script) ------------
    // Starts every peer that is not already running, then waits until all of
    // them answer the REGISTER handshake.
    int cmd_sys_start() {
        const std::string exe = proc::find_peer_exe();
        if (exe.empty()) {
            std::cerr << "peer executable not found next to the controller - build first:\n"
                      << "  cmake -S . -B build && cmake --build build --config Release\n";
            return 1;
        }
        std::error_code ec;
        fs::create_directories("run", ec);

        int rc = 0;
        std::vector<const PeerInfo*> waiting;
        for (const auto& p : peers_) {
            const long old_pid = proc::read_pid_file(proc::pid_file_for(p.id));
            if (old_pid > 0 && proc::peer_running(old_pid)) {
                std::cout << "peer " << p.id << " already running (pid " << old_pid << ")\n";
                waiting.push_back(&p);
                continue;
            }
            long pid = 0;
            std::string err;
            if (!proc::start_peer(exe, p.id, p.address, p.port, pid, err)) {
                std::cerr << "peer " << p.id << " failed to start: " << err << "\n";
                rc = 1;
                continue;
            }
            std::cout << "peer " << p.id << " started on " << p.address << ":"
                      << p.port << " (pid " << pid << ")\n";
            waiting.push_back(&p);
        }

        // Readiness: process alive AND REGISTER_ACK received (max ~10 s).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!waiting.empty() && std::chrono::steady_clock::now() < deadline) {
            for (auto it = waiting.begin(); it != waiting.end();) {
                const long pid = proc::read_pid_file(proc::pid_file_for((*it)->id));
                if (!proc::peer_running(pid)) {
                    std::cerr << "peer " << (*it)->id << " exited during start-up - see "
                              << proc::log_file_for((*it)->id) << "\n";
                    print_log_tail((*it)->id);
                    rc = 1;
                    it = waiting.erase(it);
                } else if (ping_peer(**it, 300)) {
                    it = waiting.erase(it);
                } else {
                    ++it;
                }
            }
            if (!waiting.empty())
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for (const PeerInfo* p : waiting) {
            std::cerr << "peer " << p->id << " did not become ready in time - see "
                      << proc::log_file_for(p->id) << "\n";
            print_log_tail(p->id);
            rc = 1;
        }
        if (rc == 0) std::cout << "all peers started and answering\n";
        return rc;
    }

    void print_log_tail(const std::string& id) {
        std::ifstream in(proc::log_file_for(id));
        std::string line;
        int n = 0;
        while (std::getline(in, line) && n < 20) { std::cerr << "    | " << line << "\n"; ++n; }
    }

    int cmd_sys_stop() {
        int rc = 0;
        for (const auto& p : peers_) {
            const std::string pidfile = proc::pid_file_for(p.id);
            const long pid = proc::read_pid_file(pidfile);
            if (pid <= 0) {
                std::cout << "peer " << p.id << " not running (no pid file)\n";
            } else if (!proc::peer_running(pid)) {
                std::cout << "peer " << p.id << " (pid " << pid
                          << ") not running (stale pid file removed)\n";
            } else {
                std::string err;
                if (proc::stop_peer(pid, err)) {
                    std::cout << "peer " << p.id << " (pid " << pid << ") stopped\n";
                } else {
                    std::cerr << "peer " << p.id << ": " << err << "\n";
                    rc = 1;
                    continue;  // keep the pid file: it may still be running
                }
            }
            proc::remove_pid_file(pidfile);
        }
        return rc;
    }

    // Returns 0 only if all peers are RUNNING.
    int cmd_sys_status() {
        int rc = 0;
        for (const auto& p : peers_) {
            const long pid = proc::read_pid_file(proc::pid_file_for(p.id));
            const bool alive = pid > 0 && proc::peer_running(pid);
            const char* state = "STOPPED";
            if (alive) state = ping_peer(p, 500) ? "RUNNING" : "UNRESPONSIVE";
            std::cout << "  peer " << p.id << " " << state;
            if (alive) std::cout << " (pid " << pid << ") " << p.address << ":" << p.port;
            std::cout << "\n";
            if (std::string(state) != "RUNNING") rc = 1;
        }
        return rc;
    }

    // ---- E5: recovery -------------------------------------------------------------------
    int cmd_recover(const std::string& failed_arg, bool simulate_interrupt) {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata\n";
            return 1;
        }
        // Determine which peers are offline by probing (heartbeats, not TCP
        // liveness assumptions).
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::vector<std::string> offline;
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            if (p.state == PeerState::OFFLINE) offline.push_back(p.id);
        }
        std::string failed = failed_arg;
        if (failed.empty()) {
            if (offline.empty()) {
                std::cerr << "no failed peer detected\n";
                return 1;
            }
            failed = offline.front();
        }

        RecoveryManager rm(peers_, meta_, output_dir());
        RecoveryResult r = rm.recover(failed, offline, simulate_interrupt);

        std::cout << "[E5] failed peer: " << failed << "\n";
        std::cout << "  affected chunks (" << r.affected_chunks.size() << "):";
        for (const auto& a : r.affected_chunks) std::cout << " " << a;
        std::cout << "\n  recovered (" << r.recovered_chunks.size() << "):";
        for (const auto& c : r.recovered_chunks) std::cout << " " << c;
        std::cout << "\n  retries=" << r.retry_count
                  << " integrity_failures=" << r.integrity_failures
                  << " bytes_transferred=" << r.bytes_transferred << "\n";
        if (!r.unrecoverable_chunks.empty()) {
            std::cout << "  UNRECOVERABLE:";
            for (const auto& u : r.unrecoverable_chunks) std::cout << " " << u;
            std::cout << "\n";
        }
        return r.success ? 0 : 3;
    }

    // ---- E5: reconstruction + final comparison ---------------------------------------------
    int cmd_reconstruct(const std::string& original) {
        std::string src = original;
        if (src.empty()) {
            std::ifstream in("metadata/source_path.txt");
            if (!in || !std::getline(in, src)) {
                std::cerr << "original file path unknown - pass it explicitly\n";
                return 1;
            }
        }
        RecoveryManager rm(peers_, meta_, output_dir());
        std::string recovered, err;
        if (!rm.reconstruct(src, recovered, err)) {
            std::cerr << "[E5] reconstruction failed: " << err << "\n";
            return 1;
        }
        std::cout << "[E5] reconstruction OK: " << recovered
                  << " is byte-identical to the original\n";
        return 0;
    }

    // ---- status ----------------------------------------------------------------------------
    int cmd_status() {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            std::cout << "  peer " << p.id << " " << peer_state_str(p.state) << "\n";
        }
        return 0;
    }

    // ---- reset --------------------------------------------------------------------------------
    int cmd_reset() {
        meta_.reset();
        std::error_code ec;
        for (const auto& p : peers_)
            for (const auto& e : fs::directory_iterator(p.storage_dir(), ec))
                fs::remove(e.path(), ec);
        fs::remove_all(output_dir(), ec);
        fs::create_directories(output_dir());
        std::cout << "reset done (metadata, peer storage, output cleared)\n";
        return 0;
    }

    // ---- full Gate-1 demonstration (spec §23) --------------------------------------------------
    int cmd_demo(const std::string& file_arg) {
        const std::string file = file_arg.empty() ? "input/sample_file.bin" : file_arg;
        auto step = [](const std::string& s) {
            std::cout << "\n==== " << s << " ====\n";
        };
        step("0. start / verify the 3 peer processes");
        if (cmd_sys_start() != 0) return 1;

        step("1. chunk file (E1)");
        if (cmd_chunk(file) != 0) return 1;

        step("2. distribute + replicate (E3)");
        if (cmd_distribute() != 0) return 1;

        step("3. verify all peers ONLINE (E2 REGISTER)");
        if (cmd_ping() != 0) return 1;

        step("4. simulate random single-peer failure");
        // inline to capture the randomly selected peer id
        std::string failed;
        {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            failed = peers_[pick(gen)].id;
            long pid = 0;
            if (!kill_peer_process(failed, pid)) return 1;
            json::Json d = json::Json::object();
            d["peer"] = failed;
            d["pid"] = pid;
            meta_.log_event("PEER_KILLED",
                            "peer process terminated (simulated failure)", d);
            std::cout << "[demo] randomly selected peer: " << failed
                      << " (pid " << pid << ") - terminated\n";
        }

        step("5. heartbeat failure detection (E4)");
        if (cmd_detect(true) != 0) {
            std::cerr << "failure was not detected\n";
            return 1;
        }

        step("6. recover affected chunks from surviving replicas (E5)");
        int rc = cmd_recover(failed, false);
        if (rc != 0) return rc;

        step("7. reconstruct + compare with original (E5)");
        if (cmd_reconstruct("") != 0) return 1;

        std::cout << "\n==== DEMO SUCCESS: peer " << failed
                  << " failed, all chunks recovered, file reconstructed "
                     "byte-identical ====\n";
        std::cout << "(peers are still running; use 'controller sys stop' to stop them)\n";
        return 0;
    }

    // ---- Gate-1 tests T01..T07 (spec §12) ------------------------------------------------
    // In-process equivalent of tests/run_tests.sh: identical scenarios, but it
    // drives the very same code paths (sys start/stop, fail, detect, recover,
    // reconstruct) without needing bash or PowerShell, so it behaves the same
    // on Windows and Linux.
    int cmd_test(const std::vector<std::string>& only) {
        struct Capture {
            std::ostringstream buf;
            std::streambuf* o;
            std::streambuf* e;
            Capture() : o(std::cout.rdbuf(buf.rdbuf())), e(std::cerr.rdbuf(buf.rdbuf())) {}
            ~Capture() { std::cout.rdbuf(o); std::cerr.rdbuf(e); }
        };
        auto quiet = [](const std::function<int()>& f, std::string* out = nullptr) {
            Capture c;
            const int rc = f();
            if (out) *out = c.buf.str();
            return rc;
        };
        auto slurp = [](const std::string& path) {
            std::ifstream in(fs::u8path(path), std::ios::binary);
            return std::string((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        };
        auto identical = [&](const std::string& a, const std::string& b) {
            std::error_code ec;
            return fs::is_regular_file(fs::u8path(b), ec) && slurp(a) == slurp(b);
        };
        auto events_contain = [&](const std::string& needle) {
            return slurp("metadata/events.json").find(needle) != std::string::npos;
        };
        const std::string in_file = "input/test_input.bin";
        const std::string out_file = "output/recovered_test_input.bin";

        auto fresh = [&]() {
            quiet([&] { return cmd_sys_stop(); });
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            quiet([&] { return cmd_reset(); });
            if (quiet([&] { return cmd_sys_start(); }) != 0) return false;
            std::error_code ec;
            fs::create_directories("input", ec);
            std::vector<char> data(300000);
            std::random_device rd;
            std::mt19937_64 gen(rd());
            for (auto& byte : data) byte = static_cast<char>(gen() & 0xFF);
            std::ofstream out(in_file, std::ios::binary | std::ios::trunc);
            out.write(data.data(), static_cast<std::streamsize>(data.size()));
            return static_cast<bool>(out);
        };
        auto chunk_and_distribute = [&]() {
            return quiet([&] { return cmd_chunk(in_file); }) == 0 &&
                   quiet([&] { return cmd_distribute(); }) == 0;
        };
        auto fail = [&](const std::string& id) {
            return quiet([&] { return cmd_fail(id); }) == 0;
        };
        auto count_chunks = [&](const std::string& peer, const std::string& fid) {
            int n = 0;
            std::error_code ec;
            for (const auto& e : fs::directory_iterator("storage/peer_" + peer + "/chunks", ec))
                if (e.path().filename().string().rfind(fid, 0) == 0 &&
                    e.path().extension() == ".chunk") ++n;
            return n;
        };

        using Result = std::pair<bool, std::string>;
        auto t01 = [&]() -> Result {
            if (!fresh()) return {false, "could not start peers"};
            if (quiet([&] { return cmd_chunk(in_file); }) != 0) return {false, "chunking failed"};
            if (quiet([&] { return cmd_distribute(); }) != 0) return {false, "distribution failed"};
            const std::string fid = meta_.file_id();
            const int a = count_chunks("A", fid), b = count_chunks("B", fid), c = count_chunks("C", fid);
            if (a == 3 && b == 4 && c == 3)
                return {true, "round-robin placement A=3,B=4,C=3, replication factor 2"};
            return {false, "unexpected placement A=" + std::to_string(a) + ",B=" +
                               std::to_string(b) + ",C=" + std::to_string(c)};
        };
        auto t02 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B")) return {false, "could not fail peer B"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("B", false); }) != 0) return {false, "recovery failed"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "single peer failure: detected, recovered, byte-identical"};
            return {false, "reconstruction mismatch after peer B failure"};
        };
        auto t03 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("C")) return {false, "could not fail peer C"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("C", true); }) != 0) return {false, "recovery failed"};
            if (!events_contain("TRANSFER_INTERRUPTED")) return {false, "no TRANSFER_INTERRUPTED event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "interrupted transfer discarded, full retry succeeded"};
            return {false, "reconstruction failed after retry"};
        };
        auto t04 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            const std::string path = "storage/peer_A/chunks/" + meta_.file_id() + "-C001.chunk";
            {
                // corrupt peer A's copy of chunk 1; B's copy stays intact
                std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
                if (!f) return {false, "cannot open " + path};
                f.write("CORRUPTED", 9);
            }
            if (quiet([&] { return cmd_recover("A", false); }) != 0) return {false, "recovery failed"};
            if (!events_contain("INTEGRITY_FAILURE")) return {false, "no INTEGRITY_FAILURE event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "SHA-256 mismatch rejected; recovered from second source"};
            return {false, "reconstruction failed after integrity rejection"};
        };
        auto t05 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A") || !fail("B")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            const int rc = quiet([&] { return cmd_recover("A", false); });
            if (rc == 3 && events_contain("UNRECOVERABLE")) {
                if (quiet([&] { return cmd_reconstruct(""); }) != 0)
                    return {true, "no surviving source -> UNRECOVERABLE reported, reconstruction refused"};
                return {false, "reconstruction should have been refused"};
            }
            return {false, "expected UNRECOVERABLE (rc=" + std::to_string(rc) + ")"};
        };
        auto t06 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A")) return {false, "could not fail peer A"};
            quiet([&] { return cmd_detect(true); });
            quiet([&] { return cmd_recover("A", false); });
            const int rc = quiet([&] { return cmd_reconstruct(""); });
            if (rc == 0 && identical(in_file, out_file) &&
                events_contain("\"byte_identical\": true"))
                return {true, "reconstruction in sequence order, byte-identical to original"};
            return {false, "reconstruction comparison failed (rc=" + std::to_string(rc) + ")"};
        };
        auto t07 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B") || !fail("C")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            std::string out;
            const int rc = quiet([&] { return cmd_recover("B", false); }, &out);
            // rf=2: chunks held by B+C (C2, C5) have no surviving source; the
            // rest must still be recovered and the loss reported explicitly.
            if (rc == 3 && out.find("UNRECOVERABLE") != std::string::npos &&
                out.find("recovered (3)") != std::string::npos)
                return {true, "two-peer failure: surviving chunks recovered, rest UNRECOVERABLE"};
            return {false, "unexpected result for two-peer failure (rc=" + std::to_string(rc) + ")"};
        };

        auto t08 = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            quiet([&] { return cmd_reset(); });

            const uint16_t test_coord_port = 5500;
            const std::string test_coord_addr = "127.0.0.1";
            CoordinatorService coord("127.0.0.1", test_coord_port, 250, 600, 2);
            if (!coord.start()) return {false, "could not start coordinator service"};

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) {
                coord.stop();
                return {false, "peer executable not found"};
            }

            // Start 3 peers configured to dynamically register with coordinator
            std::vector<long> pids;
            std::vector<std::string> ids = {"M2_A", "M2_B", "M2_C"};
            std::vector<uint16_t> ports = {5501, 5502, 5503};
            for (size_t i = 0; i < 3; ++i) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, ids[i], "127.0.0.1", ports[i], pid, err,
                                      test_coord_addr, test_coord_port)) {
                    coord.stop();
                    for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                    return {false, "failed to start peer " + ids[i] + ": " + err};
                }
                pids.push_back(pid);
            }

            // Wait up to 4 seconds for all 3 peers to register with coordinator
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (coord.registry().get_online_peers().size() < 3 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            auto online = coord.registry().get_online_peers();
            if (online.size() != 3) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "expected 3 online peers, got " + std::to_string(online.size())};
            }

            // Verify workload info (available storage > 0, active transfers == 0)
            for (const auto& peer_item : online) {
                if (peer_item.workload.available_storage_bytes == 0) {
                    for (long pid_val : pids) { std::string e; proc::terminate_peer(pid_val, e); }
                    coord.stop();
                    return {false, "peer " + peer_item.id + " reported 0 available storage"};
                }
                if (peer_item.workload.active_transfers != 0) {
                    for (long pid_val : pids) { std::string e; proc::terminate_peer(pid_val, e); }
                    coord.stop();
                    return {false, "peer " + peer_item.id + " active transfers non-zero"};
                }
            }

            // Terminate Peer M2_B (simulate failure)
            std::string terr;
            proc::terminate_peer(pids[1], terr);

            // Wait for failure detection: timeout=600ms, threshold=2 missed -> ~1500ms
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));

            PeerInfo peer_b;
            if (!coord.registry().get_peer("M2_B", peer_b)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peer M2_B not in registry"};
            }

            if (peer_b.state != PeerHealth::OFFLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peer M2_B expected OFFLINE, got " + std::string(peer_health_str(peer_b.state))};
            }

            // Verify M2_A and M2_C are still ONLINE
            PeerInfo peer_a, peer_c;
            coord.registry().get_peer("M2_A", peer_a);
            coord.registry().get_peer("M2_C", peer_c);
            if (peer_a.state != PeerHealth::ONLINE || peer_c.state != PeerHealth::ONLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peers A/C did not remain ONLINE during peer B failure"};
            }

            // Re-start Peer M2_B to verify re-registration and recovery to ONLINE
            long new_b_pid = 0;
            std::string err;
            if (!proc::start_peer(exe, "M2_B", "127.0.0.1", 5502, new_b_pid, err,
                                  test_coord_addr, test_coord_port)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to re-start peer M2_B: " + err};
            }
            pids[1] = new_b_pid;

            // Wait for re-registration
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            coord.registry().get_peer("M2_B", peer_b);
            if (peer_b.state != PeerHealth::ONLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "re-started peer M2_B did not return to ONLINE"};
            }

            // Cleanup
            for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
            coord.stop();
            return {true, "dynamic registration, storage reporting, heartbeats & state transitions verified"};
        };

        auto t09_rs = [&]() -> Result {
            const std::string test_in = "input/rs_test_input.bin";
            std::error_code ec;
            fs::create_directories("input", ec);
            fs::create_directories("output/rs_test_shards", ec);

            std::vector<uint8_t> test_data(150000);
            for (size_t i = 0; i < test_data.size(); ++i) {
                test_data[i] = static_cast<uint8_t>((i * 73 + 19) & 0xFF);
            }
            {
                std::ofstream out(test_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(test_data.data()), static_cast<std::streamsize>(test_data.size()));
            }

            AdaptiveChunker chunker;
            uint64_t chosen_shard_size = 0;
            auto shards = chunker.encode_file("F_RS_TEST", test_in, "output/rs_test_shards", chosen_shard_size);

            if (chosen_shard_size != 65536) {
                return {false, "expected 64KB shard size for 150KB file, got " + std::to_string(chosen_shard_size)};
            }

            // Verify reconstruction from Pair 1: D0 + D1 (P0 erased)
            {
                std::string staging = "output/rs_test_pair_d0_d1";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.shard_type == "DATA") {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d0_d1.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D0+D1: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D0+D1 is not byte-identical"};
                }
            }

            // Verify reconstruction from Pair 2: D0 + P0 (D1 erased)
            {
                std::string staging = "output/rs_test_pair_d0_p0";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D1") == std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d0_p0.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D0+P0: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D0+P0 is not byte-identical"};
                }
            }

            // Verify reconstruction from Pair 3: D1 + P0 (D0 erased)
            {
                std::string staging = "output/rs_test_pair_d1_p0";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D0") == std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d1_p0.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D1+P0: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D1+P0 is not byte-identical"};
                }
            }

            // Verify failure when only 1 shard is present
            {
                std::string staging = "output/rs_test_single_shard";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D0") != std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                        break;
                    }
                }
                std::string out_rec = "output/rs_rec_single.bin";
                std::string err;
                if (chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "expected reconstruction to fail with only 1 shard, but reported success"};
                }
            }

            return {true, "RS(3,2) systematic encoding & reconstruction verified across D0+D1, D0+P0, D1+P0 pairs"};
        };

        auto t10_gate2_e2e = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            // 1. Start Coordinator Service on port 5600
            CoordinatorService coord("127.0.0.1", 5600, 200, 500, 2);
            if (!coord.start()) {
                return {false, "failed to start Gate-2 Coordinator Service on port 5600"};
            }

            // 2. Start 3 Peer processes dynamically registering with coordinator (127.0.0.1:5600)
            std::vector<std::pair<std::string, uint16_t>> peer_cfgs = {
                {"G2_A", 5601}, {"G2_B", 5602}, {"G2_C", 5603}
            };
            std::vector<long> pids;
            for (const auto& pc : peer_cfgs) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pc.first, "127.0.0.1", pc.second, pid, err, "127.0.0.1", 5600)) {
                    for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                    coord.stop();
                    return {false, "failed to start peer " + pc.first + ": " + err};
                }
                pids.push_back(pid);
            }

            // Wait for dynamic registration
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            coord.registry().evaluate_health(500, 2);
            auto online = coord.registry().get_online_peers();
            if (online.size() != 3) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "expected 3 dynamically registered online peers, got " + std::to_string(online.size())};
            }

            // 3. Stage a file centrally in Coordinator catalog
            fs::create_directories("output");
            const std::string test_in = "output/gate2_e2e_input.bin";
            std::vector<uint8_t> test_bytes(140000);
            for (size_t i = 0; i < test_bytes.size(); ++i) {
                test_bytes[i] = static_cast<uint8_t>((i * 73 + 19) & 0xFF);
            }
            {
                std::ofstream out(test_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(test_bytes.data()), static_cast<std::streamsize>(test_bytes.size()));
            }

            FileRecord rec;
            std::string stage_err;
            if (!coord.catalog().stage_file(test_in, rec, stage_err)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to stage file: " + stage_err};
            }
            if (rec.status != FileDistributionStatus::STORED) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "initial file status is not STORED"};
            }

            // 4. Test on-demand first download with direct peer retrieval & local RS(3,2) reconstruction
            ClientDownload client("127.0.0.1", 5600, "output/gate2_e2e_client");
            DownloadResult dl_res = client.download(rec.file_id, DownloadSource::PEERS, &coord);
            if (!dl_res.success) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 peer-based download & RS reconstruction failed: " + dl_res.message};
            }

            if (dl_res.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 reconstructed file SHA-256 mismatch"};
            }

            // Verify status transitioned to AVAILABLE
            FileRecord post_dist_rec;
            coord.catalog().get_file(rec.file_id, post_dist_rec);
            if (post_dist_rec.status != FileDistributionStatus::AVAILABLE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, std::string("expected file status AVAILABLE after first download, got ") + file_status_str(post_dist_rec.status)};
            }

            // 5. Test central fallback (--source central)
            DownloadResult central_res = client.download(rec.file_id, DownloadSource::CENTRAL, &coord);
            if (!central_res.success || central_res.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 central fallback download failed: " + central_res.message};
            }

            // 6. Test Failure & Peer-to-Peer Durability Repair
            // Terminate Peer G2_B (pids[1])
            std::string term_err;
            proc::terminate_peer(pids[1], term_err);

            // Start replacement Peer G2_D
            long pid_d = 0;
            std::string err_d;
            if (!proc::start_peer(exe, "G2_D", "127.0.0.1", 5604, pid_d, err_d, "127.0.0.1", 5600)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to start replacement peer G2_D: " + err_d};
            }
            pids.push_back(pid_d);

            // Wait for Peer G2_B to be detected as OFFLINE and Peer G2_D to register ONLINE
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
            coord.registry().evaluate_health(500, 2);

            std::string repair_log;
            bool rep_ok = coord.repair_file(rec.file_id, repair_log);
            if (!rep_ok) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Coordinator repair_file failed: " + repair_log};
            }

            // Download again from surviving peers (now including repaired peer G2_D)
            DownloadResult dl_post_repair = client.download(rec.file_id, DownloadSource::PEERS, &coord);
            if (!dl_post_repair.success || dl_post_repair.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Download after repair failed: " + dl_post_repair.message};
            }

            // Cleanup
            for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
            coord.stop();
            return {true, "Gate-2 E2E verified: On-demand RS(3,2) distribution, direct peer retrieval, central fallback, and peer-to-peer durability repair"};
        };

        auto t11_peer_foundation = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer executable not found"};

            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_A", 5701},
                {"P2P_B", 5702},
                {"P2P_C", 5703},
                {"P2P_D", 5704}
            };
            const uint16_t discovery_port = 5705;

            // Clean any prior test directories for P2P_A..P2P_D
            std::error_code ec;
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }

            // 1. Launch 4 independent peer processes with NO central coordinator ("p2p" mode)
            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid > 0) {
                        std::string e;
                        proc::terminate_peer(pid, e);
                    }
                }
            };

            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch decentralized peer " + pp.first + ": " + err};
                }
                pids.push_back(pid);
            }

            // Helper to query a peer's live membership table & SQLite state over TCP
            auto query_peer_mesh = [](uint16_t port, json::Json& resp_out) -> bool {
                TCPClient client(1000, 1000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_QUERY_PEERS;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_QUERY_PEERS_ACK;
            };

            // 2. Wait up to 4 seconds for all 4 peers to discover each other via UDP + TCP handshake
            bool all_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int ready_count = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++ready_count;
                    }
                }
                if (ready_count == 4) {
                    all_discovered = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }

            if (!all_discovered) {
                cleanup_peers();
                return {false, "4 decentralized peers did not complete full mutual discovery within timeout"};
            }

            // 3. Verify each peer created its own SQLite database (storage/peer_<ID>/metadata.db)
            //    and persisted all 4 peers + local config
            for (const auto& pp : p2p_peers) {
                const std::string db_file = "storage/peer_" + pp.first + "/metadata.db";
                if (!fs::exists(db_file)) {
                    cleanup_peers();
                    return {false, "missing SQLite database file: " + db_file};
                }

                DatabaseManager inspect_db;
                std::string db_err;
                if (!inspect_db.open(db_file, &db_err)) {
                    cleanup_peers();
                    return {false, "could not open peer SQLite DB " + db_file + ": " + db_err};
                }
                if (inspect_db.get_config("peer_id") != pp.first ||
                    inspect_db.get_config("network_mode") != "DECENTRALIZED_P2P") {
                    cleanup_peers();
                    return {false, "SQLite local_config mismatch on " + pp.first};
                }
                const auto db_peers = inspect_db.get_all_peers(true);
                if (db_peers.size() != 4) {
                    cleanup_peers();
                    return {false, "expected 4 peers in SQLite DB of " + pp.first +
                                       ", got " + std::to_string(db_peers.size())};
                }
                for (const auto& dp : db_peers) {
                    if (dp.state != PeerHealth::ONLINE) {
                        cleanup_peers();
                        return {false, "peer " + dp.id + " not ONLINE in SQLite DB of " + pp.first};
                    }
                    if (dp.workload.available_storage_bytes == 0) {
                        cleanup_peers();
                        return {false, "peer " + dp.id + " has 0 storage capacity in SQLite DB of " + pp.first};
                    }
                }
                inspect_db.close();
            }

            // 4. Verify direct TCP P2P_HANDSHAKE exchange with each peer
            for (const auto& pp : p2p_peers) {
                TCPClient c(1000, 1000);
                if (!c.connect_to("127.0.0.1", pp.second)) {
                    cleanup_peers();
                    return {false, "direct TCP connect failed to " + pp.first};
                }
                json::Json hs_req = json::Json::object();
                hs_req["type"] = proto::P2P_HANDSHAKE;
                hs_req["peer_id"] = "P2P_A";
                hs_req["address"] = "127.0.0.1";
                hs_req["port"] = 5701LL;
                hs_req["available_storage_bytes"] = 1024LL * 1024LL;
                hs_req["active_transfers"] = 0LL;
                hs_req["transfer_capacity"] = 10LL;

                json::Json hs_resp;
                std::vector<uint8_t> pl;
                if (c.request(hs_req, {}, hs_resp, pl) != proto::RecvStatus::OK ||
                    hs_resp["type"].as_string() != proto::P2P_HANDSHAKE_ACK ||
                    hs_resp["peer_id"].as_string() != pp.first ||
                    hs_resp["status"].as_string() != "OK") {
                    cleanup_peers();
                    return {false, "P2P_HANDSHAKE verification failed with " + pp.first};
                }
            }

            // 5. Terminate P2P_B (index 1) and verify remaining 3 peers (A, C, D)
            //    independently transition P2P_B to OFFLINE in both memory and SQLite
            {
                std::string term_err;
                proc::terminate_peer(pids[1], term_err);
                pids[1] = 0;
            }

            bool all_detected_offline = false;
            const auto fail_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
            while (std::chrono::steady_clock::now() < fail_deadline) {
                int confirmed = 0;
                for (size_t i : {size_t(0), size_t(2), size_t(3)}) {
                    json::Json resp;
                    if (query_peer_mesh(p2p_peers[i].second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array()) {
                        bool b_offline = false;
                        int others_online = 0;
                        for (const auto& item : resp["peers"].items()) {
                            const std::string pid_str = item["id"].as_string();
                            const std::string h_str = item["health"].as_string();
                            if (pid_str == "P2P_B" && h_str == "OFFLINE") b_offline = true;
                            else if (pid_str != "P2P_B" && h_str == "ONLINE") ++others_online;
                        }
                        if (b_offline && others_online == 3) ++confirmed;
                    }
                }
                if (confirmed == 3) {
                    all_detected_offline = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }

            if (!all_detected_offline) {
                cleanup_peers();
                return {false, "surviving peers A, C, D did not all mark P2P_B OFFLINE"};
            }

            // Confirm SQLite databases on A, C, D persisted P2P_B as OFFLINE
            for (size_t i : {size_t(0), size_t(2), size_t(3)}) {
                DatabaseManager inspect_db("storage/peer_" + p2p_peers[i].first + "/metadata.db");
                PeerInfo b_row;
                if (!inspect_db.get_peer("P2P_B", b_row) || b_row.state != PeerHealth::OFFLINE) {
                    cleanup_peers();
                    return {false, "SQLite DB on " + p2p_peers[i].first + " did not persist P2P_B as OFFLINE"};
                }
            }

            // 6. Restart P2P_B and verify it loads its existing SQLite DB, rejoins the mesh,
            //    and all 4 peers mark P2P_B as ONLINE again in memory and SQLite
            {
                long new_b_pid = 0;
                std::string err;
                if (!proc::start_peer(exe, "P2P_B", "127.0.0.1", 5702, new_b_pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to restart P2P_B: " + err};
                }
                pids[1] = new_b_pid;
            }

            bool all_rejoined = false;
            const auto rejoin_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3500);
            while (std::chrono::steady_clock::now() < rejoin_deadline) {
                int confirmed = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++confirmed;
                    }
                }
                if (confirmed == 4) {
                    all_rejoined = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }

            if (!all_rejoined) {
                cleanup_peers();
                return {false, "restarted peer P2P_B did not rejoin as ONLINE across all 4 peers"};
            }

            // Verify SQLite persistence after rejoin across all 4 peers
            for (const auto& pp : p2p_peers) {
                DatabaseManager inspect_db("storage/peer_" + pp.first + "/metadata.db");
                PeerInfo b_row;
                int64_t last_seen = 0;
                if (!inspect_db.get_peer("P2P_B", b_row, &last_seen) ||
                    b_row.state != PeerHealth::ONLINE || last_seen <= 0) {
                    cleanup_peers();
                    return {false, "SQLite DB on " + pp.first + " did not update P2P_B back to ONLINE"};
                }
            }

            cleanup_peers();
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "4-peer decentralized foundation verified: UDP discovery, TCP handshake, per-peer SQLite DBs, heartbeats, OFFLINE detection & rejoin"};
        };

        auto t12_distributed_metadata = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer executable not found"};

            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_A", 5801},
                {"P2P_B", 5802},
                {"P2P_C", 5803},
                {"P2P_D", 5804}
            };
            const uint16_t discovery_port = 5805;

            std::error_code ec;
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }

            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid > 0) {
                        std::string e;
                        proc::terminate_peer(pid, e);
                    }
                }
            };

            // 1. Launch 4 peers without any central coordinator
            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch decentralized peer " + pp.first + ": " + err};
                }
                pids.push_back(pid);
            }

            auto query_peer_mesh = [](uint16_t port, json::Json& resp_out) -> bool {
                TCPClient client(1000, 1000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_QUERY_PEERS;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_QUERY_PEERS_ACK;
            };

            auto push_manifest_to_peer = [](uint16_t port,
                                            const FileManifest& manifest,
                                            const std::string& origin_peer,
                                            bool propagate) -> bool {
                TCPClient client(1000, 1000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_METADATA_PUSH;
                req["origin_peer_id"] = origin_peer;
                req["propagate"] = propagate;
                req["manifest"] = manifest.to_json();
                json::Json resp;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                       resp["type"].as_string() == proto::P2P_METADATA_PUSH_ACK &&
                       resp["status"].as_string() == "OK";
            };

            auto query_peer_catalogue = [](uint16_t port,
                                           const std::string& query_str,
                                           const std::string& exact_file_id,
                                           json::Json& resp_out) -> bool {
                TCPClient client(1000, 1000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_CATALOGUE_QUERY;
                if (!query_str.empty()) req["query"] = query_str;
                if (!exact_file_id.empty()) req["file_id"] = exact_file_id;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_CATALOGUE_QUERY_ACK &&
                       resp_out["status"].as_string() == "OK";
            };

            // Wait for 4-peer mutual discovery
            bool all_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int ready_count = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++ready_count;
                    }
                }
                if (ready_count == 4) {
                    all_discovered = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!all_discovered) {
                cleanup_peers();
                return {false, "4 decentralized peers did not complete discovery in T12"};
            }

            // 2. Build synthetic manifest #1 (F_SYNTH_001: 2 stripes, 6 RS(3,2) shards) on Peer A
            const int64_t base_ts = DatabaseManager::now_epoch_ms();
            FileManifest m1;
            m1.file_id = "F_SYNTH_001";
            m1.original_name = "orbital_telemetry_2026.bin";
            m1.file_size = 200000;
            m1.sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
            m1.shard_size = 65536;
            m1.data_shards_k = 2;
            m1.parity_shards_m = 1;
            m1.total_stripes = 2;
            m1.total_shards = 6;
            m1.uploader_peer_id = "P2P_A";
            m1.uploaded_at_epoch_ms = base_ts;
            m1.status = "AVAILABLE";
            m1.version = 1;

            m1.stripes = {
                {"F_SYNTH_001", 0, 131072, 65536},
                {"F_SYNTH_001", 1, 68928, 65536}
            };

            auto make_shard = [&](int32_t stripe_idx, int32_t shard_idx, const std::string& type,
                                  uint64_t orig_bytes, const std::string& host_peer) {
                ShardManifestEntry s;
                const std::string suffix = (shard_idx == 0 ? "D0" : (shard_idx == 1 ? "D1" : "P0"));
                s.shard_id = "F_SYNTH_001-S00" + std::to_string(stripe_idx) + "-" + suffix;
                s.file_id = "F_SYNTH_001";
                s.stripe_index = stripe_idx;
                s.shard_index = shard_idx;
                s.shard_type = type;
                s.shard_size = 65536;
                s.original_bytes = orig_bytes;
                s.sha256 = "sha256_" + s.shard_id;

                ShardLocationEntry loc;
                loc.shard_id = s.shard_id;
                loc.peer_id = host_peer;
                loc.location_status = "STORED";
                loc.verified_at_epoch_ms = base_ts;
                loc.version = 1;
                s.locations.push_back(loc);
                return s;
            };

            // Stripe 0 shards on A, B, C; Stripe 1 shards on B, C, D
            m1.shards.push_back(make_shard(0, 0, "DATA",   65536, "P2P_A"));
            m1.shards.push_back(make_shard(0, 1, "DATA",   65536, "P2P_B"));
            m1.shards.push_back(make_shard(0, 2, "PARITY", 65536, "P2P_C"));
            m1.shards.push_back(make_shard(1, 0, "DATA",   65536, "P2P_B"));
            m1.shards.push_back(make_shard(1, 1, "DATA",    3392, "P2P_C"));
            m1.shards.push_back(make_shard(1, 2, "PARITY", 65536, "P2P_D"));

            // Publish m1 on Peer A (5801)
            if (!push_manifest_to_peer(5801, m1, "P2P_A", true)) {
                cleanup_peers();
                return {false, "failed to push initial manifest F_SYNTH_001 to P2P_A"};
            }

            // Wait up to 3 seconds for F_SYNTH_001 to synchronize to all 4 peers (A, B, C, D)
            bool m1_synced_all = false;
            const auto sync1_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < sync1_deadline) {
                int have_m1 = 0;
                for (const auto& pp : p2p_peers) {
                    DatabaseManager db("storage/peer_" + pp.first + "/metadata.db");
                    FileManifest got;
                    if (db.get_file_manifest("F_SYNTH_001", got) &&
                        got.version == 1 &&
                        got.stripes.size() == 2 &&
                        got.shards.size() == 6) {
                        ++have_m1;
                    }
                }
                if (have_m1 == 4) {
                    m1_synced_all = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (!m1_synced_all) {
                cleanup_peers();
                return {false, "manifest F_SYNTH_001 created on P2P_A did not propagate to B, C, and D"};
            }

            // 3. Verify all 4 databases contain exact expected relational rows
            for (const auto& pp : p2p_peers) {
                DatabaseManager db("storage/peer_" + pp.first + "/metadata.db");
                if (db.count_rows("files") != 1 ||
                    db.count_rows("stripes") != 2 ||
                    db.count_rows("shards") != 6 ||
                    db.count_rows("shard_locations") != 6) {
                    cleanup_peers();
                    return {false, "unexpected table row counts on " + pp.first + " after v1 sync"};
                }
            }

            // 4. Verify a new metadata update (version 2) propagates across all peers
            FileManifest m1_v2 = m1;
            m1_v2.version = 2;
            ShardLocationEntry extra_loc;
            extra_loc.shard_id = "F_SYNTH_001-S000-D0";
            extra_loc.peer_id = "P2P_D";
            extra_loc.location_status = "STORED";
            extra_loc.verified_at_epoch_ms = base_ts + 500;
            extra_loc.version = 2;
            m1_v2.shards[0].locations.push_back(extra_loc);

            if (!push_manifest_to_peer(5801, m1_v2, "P2P_A", true)) {
                cleanup_peers();
                return {false, "failed to push v2 update of F_SYNTH_001 to P2P_A"};
            }

            bool v2_synced_all = false;
            const auto sync2_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < sync2_deadline) {
                int have_v2 = 0;
                for (const auto& pp : p2p_peers) {
                    DatabaseManager db("storage/peer_" + pp.first + "/metadata.db");
                    FileManifest got;
                    if (db.get_file_manifest("F_SYNTH_001", got) &&
                        got.version == 2 &&
                        db.count_rows("shard_locations") == 7) {
                        ++have_v2;
                    }
                }
                if (have_v2 == 4) {
                    v2_synced_all = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (!v2_synced_all) {
                cleanup_peers();
                return {false, "version 2 metadata update of F_SYNTH_001 did not propagate to all 4 peers"};
            }

            // 5. Verify duplicate messages do not create duplicate records (idempotency check)
            for (int dup = 0; dup < 4; ++dup) {
                push_manifest_to_peer(5801, m1_v2, "P2P_A", true);
                push_manifest_to_peer(5802, m1_v2, "P2P_A", false);
                // Also send older version v1 to confirm version monotonic protection
                push_manifest_to_peer(5802, m1, "P2P_A", false);
            }

            for (const auto& pp : p2p_peers) {
                DatabaseManager db("storage/peer_" + pp.first + "/metadata.db");
                FileManifest got;
                if (!db.get_file_manifest("F_SYNTH_001", got) || got.version != 2) {
                    cleanup_peers();
                    return {false, "version regressed or manifest missing on " + pp.first + " after duplicate/stale pushes"};
                }
                if (db.count_rows("files") != 1 ||
                    db.count_rows("stripes") != 2 ||
                    db.count_rows("shards") != 6 ||
                    db.count_rows("shard_locations") != 7 ||
                    db.count_rows("metadata_sync_log") != 2) {
                    cleanup_peers();
                    return {false, "duplicate messages created duplicate database rows on " + pp.first};
                }
            }

            // 6. Stop P2P_B (offline peer scenario), verify degraded reconstructability awareness,
            //    publish F_SYNTH_002 on P2P_C while P2P_B is offline, then restart P2P_B
            {
                std::string term_err;
                proc::terminate_peer(pids[1], term_err);
                pids[1] = 0;
            }

            // Wait for surviving peers (A, C, D) to mark P2P_B OFFLINE
            bool b_marked_offline = false;
            const auto off_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < off_deadline) {
                DatabaseManager db_d("storage/peer_P2P_D/metadata.db");
                PeerInfo b_info;
                if (db_d.get_peer("P2P_B", b_info) && b_info.state == PeerHealth::OFFLINE) {
                    b_marked_offline = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!b_marked_offline) {
                cleanup_peers();
                return {false, "P2P_D did not mark P2P_B OFFLINE in T12"};
            }

            // Query Peer D's catalogue while P2P_B is OFFLINE:
            // F_SYNTH_001 has Stripe 0 (shards on A+D, B[offline], C) -> 2/3 online
            // and Stripe 1 (shards on B[offline], C, D) -> 2/3 online.
            // So live_status must be DEGRADED and reconstructable == true!
            {
                json::Json cat_resp;
                if (!query_peer_catalogue(5804, "", "F_SYNTH_001", cat_resp) ||
                    !cat_resp.contains("files") || cat_resp["files"].size() != 1) {
                    cleanup_peers();
                    return {false, "catalogue query for F_SYNTH_001 on P2P_D failed while P2P_B offline"};
                }
                const auto& f0 = cat_resp["files"][0];
                if (!f0["reconstructable"].as_bool() || f0["live_status"].as_string() != "DEGRADED") {
                    cleanup_peers();
                    return {false, "expected F_SYNTH_001 live_status=DEGRADED & reconstructable=true when P2P_B is offline, got " +
                                       f0["live_status"].as_string()};
                }
            }

            // Publish a second manifest F_SYNTH_002 on Peer C (5803) while P2P_B is OFFLINE
            FileManifest m2;
            m2.file_id = "F_SYNTH_002";
            m2.original_name = "deep_space_image_v2.raw";
            m2.file_size = 98304;
            m2.sha256 = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
            m2.shard_size = 65536;
            m2.data_shards_k = 2;
            m2.parity_shards_m = 1;
            m2.total_stripes = 1;
            m2.total_shards = 3;
            m2.uploader_peer_id = "P2P_C";
            m2.uploaded_at_epoch_ms = base_ts + 2000;
            m2.status = "AVAILABLE";
            m2.version = 1;
            m2.stripes = {{"F_SYNTH_002", 0, 98304, 65536}};

            auto make_m2_shard = [&](int32_t shard_idx, const std::string& type,
                                     uint64_t orig_bytes, const std::string& host_peer) {
                ShardManifestEntry s;
                const std::string suffix = (shard_idx == 0 ? "D0" : (shard_idx == 1 ? "D1" : "P0"));
                s.shard_id = "F_SYNTH_002-S000-" + suffix;
                s.file_id = "F_SYNTH_002";
                s.stripe_index = 0;
                s.shard_index = shard_idx;
                s.shard_type = type;
                s.shard_size = 65536;
                s.original_bytes = orig_bytes;
                s.sha256 = "sha256_" + s.shard_id;

                ShardLocationEntry loc;
                loc.shard_id = s.shard_id;
                loc.peer_id = host_peer;
                loc.location_status = "STORED";
                loc.verified_at_epoch_ms = base_ts + 2000;
                loc.version = 1;
                s.locations.push_back(loc);
                return s;
            };
            m2.shards.push_back(make_m2_shard(0, "DATA",   65536, "P2P_A"));
            m2.shards.push_back(make_m2_shard(1, "DATA",   32768, "P2P_C"));
            m2.shards.push_back(make_m2_shard(2, "PARITY", 65536, "P2P_D"));

            if (!push_manifest_to_peer(5803, m2, "P2P_C", true)) {
                cleanup_peers();
                return {false, "failed to push F_SYNTH_002 to P2P_C while P2P_B was offline"};
            }

            // Restart P2P_B (5802) and verify:
            //   (a) P2P_B restores its existing SQLite database (F_SYNTH_001 v2)
            //   (b) P2P_B synchronizes and receives missing metadata F_SYNTH_002 after rejoining
            {
                long new_b_pid = 0;
                std::string err;
                if (!proc::start_peer(exe, "P2P_B", "127.0.0.1", 5802, new_b_pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to restart P2P_B in T12: " + err};
                }
                pids[1] = new_b_pid;
            }

            bool b_caught_up = false;
            const auto catchup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (std::chrono::steady_clock::now() < catchup_deadline) {
                DatabaseManager db_b("storage/peer_P2P_B/metadata.db");
                FileManifest b_m1, b_m2;
                if (db_b.get_file_manifest("F_SYNTH_001", b_m1) && b_m1.version == 2 &&
                    db_b.get_file_manifest("F_SYNTH_002", b_m2) && b_m2.version == 1 &&
                    b_m2.shards.size() == 3) {
                    b_caught_up = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!b_caught_up) {
                cleanup_peers();
                return {false, "restarted peer P2P_B did not restore F_SYNTH_001 and sync missing F_SYNTH_002"};
            }

            // Wait for P2P_D to mark P2P_B ONLINE again so F_SYNTH_001 returns to AVAILABLE
            const auto b_online_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < b_online_deadline) {
                DatabaseManager db_d("storage/peer_P2P_D/metadata.db");
                PeerInfo b_info;
                if (db_d.get_peer("P2P_B", b_info) && b_info.state == PeerHealth::ONLINE) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            // 7. Verify any peer (e.g. Peer D on 5804 and Peer B on 5802) can search the distributed catalogue
            //    by filename substring, by exact File ID, and list all available files
            {
                json::Json search_by_name;
                if (!query_peer_catalogue(5804, "orbital_telemetry", "", search_by_name) ||
                    !search_by_name.contains("files") ||
                    search_by_name["files"].size() != 1 ||
                    search_by_name["files"][0]["file_id"].as_string() != "F_SYNTH_001" ||
                    !search_by_name["files"][0]["reconstructable"].as_bool() ||
                    search_by_name["files"][0]["live_status"].as_string() != "AVAILABLE") {
                    cleanup_peers();
                    return {false, "catalogue search by filename 'orbital_telemetry' on P2P_D failed"};
                }

                json::Json search_m2_on_b;
                if (!query_peer_catalogue(5802, "deep_space", "", search_m2_on_b) ||
                    !search_m2_on_b.contains("files") ||
                    search_m2_on_b["files"].size() != 1 ||
                    search_m2_on_b["files"][0]["file_id"].as_string() != "F_SYNTH_002" ||
                    !search_m2_on_b["files"][0]["reconstructable"].as_bool()) {
                    cleanup_peers();
                    return {false, "catalogue search by filename 'deep_space' on rejoined P2P_B failed"};
                }

                for (const auto& pp : p2p_peers) {
                    json::Json all_cat;
                    if (!query_peer_catalogue(pp.second, "", "", all_cat) ||
                        !all_cat.contains("files") ||
                        all_cat["files"].size() != 2) {
                        cleanup_peers();
                        return {false, "peer " + pp.first + " did not report 2 files in full catalogue query"};
                    }
                }
            }

            cleanup_peers();
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "distributed metadata & file catalogue verified: P2P manifest push, anti-entropy sync, idempotency, offline catch-up, DB persistence & any-peer search"};
        };

        auto t13_p2p_file_upload = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const uint16_t discovery_port = 5905;
            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_A", 5901},
                {"P2P_B", 5902},
                {"P2P_C", 5903},
                {"P2P_D", 5904}
            };

            std::error_code ec;
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            const std::string upload_dir = "output/t13_upload_inputs";
            fs::remove_all(upload_dir, ec);
            fs::create_directories(upload_dir, ec);

            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid != 0) {
                        std::string term_err;
                        proc::terminate_peer(pid, term_err);
                    }
                }
                pids.clear();
            };

            // 1. Launch 4 peers in decentralized mode without any central coordinator
            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch decentralized peer " + pp.first + ": " + err};
                }
                pids.push_back(pid);
            }

            auto query_peer_mesh = [](uint16_t port, json::Json& resp_out) -> bool {
                TCPClient client(1500, 1500);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_QUERY_PEERS;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_QUERY_PEERS_ACK;
            };

            auto send_upload_request = [](uint16_t port,
                                          const std::string& file_path,
                                          const std::string& file_id_override,
                                          const json::Json& extra_fields,
                                          json::Json& resp_out) -> bool {
                TCPClient client(3000, 20000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_UPLOAD_FILE;
                req["file_path"] = fs::absolute(file_path).string();
                req["filename"] = fs::path(file_path).filename().string();
                if (!file_id_override.empty()) req["file_id"] = file_id_override;
                if (extra_fields.is_object()) {
                    for (const auto& kv : extra_fields.members()) {
                        req[kv.first] = kv.second;
                    }
                }
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_UPLOAD_FILE_ACK;
            };

            // Wait for all 4 peers to discover each other and mark all 4 ONLINE
            bool all_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int ready_count = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++ready_count;
                    }
                }
                if (ready_count == 4) {
                    all_discovered = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!all_discovered) {
                cleanup_peers();
                return {false, "4 decentralized peers did not complete discovery in T13"};
            }

            // Prepare 4 distinct input files covering:
            // (a) Normal text file (uploaded via P2P_A)
            // (b) PDF file with binary stream (uploaded via P2P_B)
            // (c) Exact stripe-aligned binary file (uploaded via P2P_C)
            // (d) Arbitrary binary bytes (0x00..0xFF) requiring final stripe padding (uploaded via P2P_D)
            const std::string text_path = upload_dir + "/mission_notes.txt";
            {
                std::ofstream out(text_path, std::ios::binary | std::ios::trunc);
                for (int i = 0; i < 250; ++i) {
                    out << "Line " << i << ": Decentralized P2P File Transfer & Resilient Recovery Engine (RS 3,2) verification text.\r\n";
                }
            }

            const std::string pdf_path = upload_dir + "/engineering_spec.pdf";
            {
                std::ofstream out(pdf_path, std::ios::binary | std::ios::trunc);
                const std::string hdr = "%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\nstream\n";
                out.write(hdr.data(), static_cast<std::streamsize>(hdr.size()));
                std::vector<uint8_t> pdf_body(150000);
                for (size_t i = 0; i < pdf_body.size(); ++i) {
                    pdf_body[i] = static_cast<uint8_t>((i * 41 + 17) & 0xFF);
                }
                out.write(reinterpret_cast<const char*>(pdf_body.data()), static_cast<std::streamsize>(pdf_body.size()));
                const std::string trl = "\nendstream\nendobj\n%%EOF\n";
                out.write(trl.data(), static_cast<std::streamsize>(trl.size()));
            }

            const std::string bin_path = upload_dir + "/sensor_capture.bin";
            {
                std::vector<uint8_t> bin_body(131072); // Exact 1 full stripe (2 * 65536)
                for (size_t i = 0; i < bin_body.size(); ++i) {
                    bin_body[i] = static_cast<uint8_t>((i * 131 + 29) & 0xFF);
                }
                std::ofstream out(bin_path, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(bin_body.data()), static_cast<std::streamsize>(bin_body.size()));
            }

            const std::string padded_path = upload_dir + "/arbitrary_padded.dat";
            const size_t padded_size = 175123; // Stripe 0: 131072B (D0=65536, D1=65536), Stripe 1: 44051B (D0=44051, D1=0)
            {
                std::vector<uint8_t> arb(padded_size);
                for (size_t i = 0; i < arb.size(); ++i) {
                    // Ensure all 256 byte values 0x00..0xFF appear repeatedly
                    arb[i] = static_cast<uint8_t>((i & 0xFF) ^ ((i >> 8) * 53));
                }
                for (size_t b = 0; b < 256 && b < arb.size(); ++b) {
                    arb[b] = static_cast<uint8_t>(b);
                }
                std::ofstream out(padded_path, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(arb.data()), static_cast<std::streamsize>(arb.size()));
            }

            struct UploadCase {
                std::string file_id;
                std::string path;
                uint16_t uploader_port;
                std::string uploader_id;
                size_t expected_stripes;
            };
            const std::vector<UploadCase> cases = {
                {"F_UP_TEXT",   text_path,   5901, "P2P_A", 1},
                {"F_UP_PDF",    pdf_path,    5902, "P2P_B", 2},
                {"F_UP_BIN",    bin_path,    5903, "P2P_C", 1},
                {"F_UP_PADDED", padded_path, 5904, "P2P_D", 2}
            };

            // 2. Execute uploads across all 4 participating peers (A, B, C, D)
            for (const auto& uc : cases) {
                json::Json resp;
                if (!send_upload_request(uc.uploader_port, uc.path, uc.file_id, json::Json::object(), resp) ||
                    resp["status"].as_string() != "OK") {
                    cleanup_peers();
                    return {false, "upload of " + uc.file_id + " via " + uc.uploader_id + " failed: " +
                                       (resp.contains("reason") ? resp["reason"].as_string() : "no response")};
                }

                const std::string expected_sha = ChunkManager::sha256_of_file(uc.path);
                if (!resp.contains("manifest") || !resp["manifest"].is_object()) {
                    cleanup_peers();
                    return {false, "upload ACK missing manifest object for " + uc.file_id};
                }
                const auto& ack_m = resp["manifest"];
                if (ack_m["sha256"].as_string() != expected_sha) {
                    cleanup_peers();
                    return {false, "SHA-256 mismatch in upload ACK for " + uc.file_id};
                }
                if (static_cast<size_t>(ack_m["total_stripes"].as_int()) != uc.expected_stripes ||
                    static_cast<size_t>(ack_m["total_shards"].as_int()) != uc.expected_stripes * 3) {
                    cleanup_peers();
                    return {false, "unexpected stripe/shard counts for " + uc.file_id};
                }
            }

            // 3. Verify adaptive chunking, final stripe padding metadata, RS(3,2) mathematical parity,
            //    3 distinct peers per stripe, physical .chunk files on disk, and destination SHA-256
            std::map<std::string, int> peer_shard_counts;
            for (const auto& uc : cases) {
                DatabaseManager db_up("storage/peer_" + uc.uploader_id + "/metadata.db");
                FileManifest fm;
                if (!db_up.get_file_manifest(uc.file_id, fm)) {
                    cleanup_peers();
                    return {false, "uploader database missing manifest for " + uc.file_id};
                }
                if (fm.status != "AVAILABLE" || fm.version != 2 ||
                    fm.data_shards_k != 2 || fm.parity_shards_m != 1 || fm.shard_size != 65536) {
                    cleanup_peers();
                    return {false, "invalid manifest header fields for " + uc.file_id};
                }

                // Group shards by stripe_index
                std::map<int32_t, std::vector<ShardManifestEntry>> by_stripe;
                for (const auto& s : fm.shards) {
                    by_stripe[s.stripe_index].push_back(s);
                }
                if (by_stripe.size() != uc.expected_stripes) {
                    cleanup_peers();
                    return {false, "stripe count mismatch in manifest for " + uc.file_id};
                }

                for (auto& kv : by_stripe) {
                    auto& stripe_shards = kv.second;
                    std::sort(stripe_shards.begin(), stripe_shards.end(),
                              [](const ShardManifestEntry& a, const ShardManifestEntry& b) {
                                  return a.shard_index < b.shard_index;
                              });
                    if (stripe_shards.size() != 3 ||
                        stripe_shards[0].shard_type != "DATA" ||
                        stripe_shards[1].shard_type != "DATA" ||
                        stripe_shards[2].shard_type != "PARITY") {
                        cleanup_peers();
                        return {false, "stripe does not contain exact D0, D1, P0 shards in " + uc.file_id};
                    }

                    // Verify 3 distinct storage peers per stripe (no two shards of the same stripe on one peer)
                    std::vector<std::string> stripe_peers;
                    std::vector<std::vector<uint8_t>> shard_bytes(3);
                    for (size_t i = 0; i < 3; ++i) {
                        const auto& sh = stripe_shards[i];
                        if (sh.locations.empty() || sh.locations[0].location_status != "STORED") {
                            cleanup_peers();
                            return {false, "shard " + sh.shard_id + " missing STORED location"};
                        }
                        const std::string host_peer = sh.locations[0].peer_id;
                        if (std::find(stripe_peers.begin(), stripe_peers.end(), host_peer) != stripe_peers.end()) {
                            cleanup_peers();
                            return {false, "multiple shards of stripe placed on same peer " + host_peer};
                        }
                        stripe_peers.push_back(host_peer);
                        peer_shard_counts[host_peer]++;

                        // Verify physical shard file exists on destination peer disk and matches SHA-256
                        const std::string disk_path = "storage/peer_" + host_peer + "/chunks/" + sh.shard_id + ".chunk";
                        if (!fs::exists(disk_path, ec) || fs::file_size(disk_path, ec) != 65536) {
                            cleanup_peers();
                            return {false, "physical shard file missing or wrong size on disk: " + disk_path};
                        }
                        if (ChunkManager::sha256_of_file(disk_path) != sh.sha256) {
                            cleanup_peers();
                            return {false, "on-disk shard SHA-256 mismatch for " + disk_path};
                        }
                        const std::string raw_s = slurp(disk_path);
                        shard_bytes[i].assign(reinterpret_cast<const uint8_t*>(raw_s.data()),
                                              reinterpret_cast<const uint8_t*>(raw_s.data()) + raw_s.size());

                        // Also verify destination peer recorded this shard in its own local SQLite database
                        DatabaseManager host_db("storage/peer_" + host_peer + "/metadata.db");
                        FileManifest host_fm;
                        if (!host_db.get_file_manifest(uc.file_id, host_fm)) {
                            cleanup_peers();
                            return {false, "destination peer " + host_peer + " DB missing file " + uc.file_id};
                        }
                    }

                    // Verify RS(3,2) mathematical validity: reconstruct D0 from (D1 + P0)
                    ReedSolomon rs(2, 1);
                    std::vector<std::vector<uint8_t>> test_decode = shard_bytes;
                    std::fill(test_decode[0].begin(), test_decode[0].end(), 0);
                    std::vector<uint8_t*> ptrs = {
                        test_decode[0].data(),
                        test_decode[1].data(),
                        test_decode[2].data()
                    };
                    std::vector<bool> present = {false, true, true};
                    if (!rs.decode(ptrs, present, 65536) || test_decode[0] != shard_bytes[0]) {
                        cleanup_peers();
                        return {false, "RS(3,2) parity verification failed on stripe of " + uc.file_id};
                    }
                }

                // Specifically verify final stripe padding metadata on F_UP_PADDED (175,123 bytes)
                if (uc.file_id == "F_UP_PADDED") {
                    const auto& s0 = by_stripe[0];
                    const auto& s1 = by_stripe[1];
                    if (s0[0].original_bytes != 65536 || s0[1].original_bytes != 65536 ||
                        s1[0].original_bytes != 44051 || s1[1].original_bytes != 0) {
                        cleanup_peers();
                        return {false, "padded final stripe original_bytes metadata incorrect on F_UP_PADDED"};
                    }
                }
            }

            // Verify all 4 peers received balanced shard placements across the 18 shards (6 stripes * 3 shards)
            for (const auto& pp : p2p_peers) {
                if (peer_shard_counts[pp.first] < 3) {
                    cleanup_peers();
                    return {false, "unbalanced shard distribution: peer " + pp.first + " received only " +
                                       std::to_string(peer_shard_counts[pp.first]) + " shards"};
                }
            }

            // 4. Wait up to 3 seconds for all 4 uploaded manifests (version 2, AVAILABLE) to synchronize across all 4 peers
            bool all_4_synced = false;
            const auto sync_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < sync_deadline) {
                int synced_peers = 0;
                for (const auto& pp : p2p_peers) {
                    DatabaseManager db("storage/peer_" + pp.first + "/metadata.db");
                    int avail_count = 0;
                    for (const auto& uc : cases) {
                        FileManifest fm;
                        if (db.get_file_manifest(uc.file_id, fm) &&
                            fm.status == "AVAILABLE" && fm.version == 2 &&
                            fm.shards.size() == uc.expected_stripes * 3) {
                            ++avail_count;
                        }
                    }
                    if (avail_count == 4) ++synced_peers;
                }
                if (synced_peers == 4) {
                    all_4_synced = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (!all_4_synced) {
                cleanup_peers();
                return {false, "uploaded file manifests did not synchronize as AVAILABLE across all 4 peers"};
            }

            // 5. Verify Task 3.7 failure handling during upload:
            //    - Destination peer unavailable/rejects on first attempt (failover/retry)
            //    - Interrupted TCP transfer retry
            //    - Invalid shard hash (corrupted payload rejected by destination SHA-256 check & retried cleanly)
            //    - Duplicate STORE_CHUNK requests & repeated STORE_ACK verification (idempotent)
            {
                json::Json fault_opts = json::Json::object();
                fault_opts["fail_first_store_on_peer"] = "P2P_B";
                fault_opts["corrupt_first_hash_on_peer"] = true;
                fault_opts["simulate_interrupted_transfer"] = true;
                fault_opts["duplicate_store_count"] = static_cast<int64_t>(2);

                json::Json fault_resp;
                if (!send_upload_request(5901, text_path, "F_UP_RESILIENT", fault_opts, fault_resp) ||
                    fault_resp["status"].as_string() != "OK") {
                    cleanup_peers();
                    return {false, "resilient upload under injected faults failed: " +
                                       (fault_resp.contains("reason") ? fault_resp["reason"].as_string() : "no response")};
                }
                if (fault_resp["retries_performed"].as_int() < 3 ||
                    fault_resp["duplicate_acks_verified"].as_int() != 2) {
                    cleanup_peers();
                    return {false, "expected >=3 bounded retries and 2 duplicate ACKs verified during fault-injected upload"};
                }
            }

            // 5.5 Verify CLI rfe.exe upload and catalogue inspection against any participating peer
            {
                const std::string rfe_exe = (fs::path(exe).parent_path() / "rfe.exe").string();
                if (fs::exists(rfe_exe, ec)) {
                    const std::string cli_cmd = "\"\"" + rfe_exe + "\" upload \"" +
                                                fs::absolute(text_path).string() +
                                                "\" --peer 127.0.0.1:5902 > \"" +
                                                upload_dir + "/cli_out.txt\" 2>&1\"";
                    const int cli_rc = std::system(cli_cmd.c_str());
                    const std::string cli_out = slurp(upload_dir + "/cli_out.txt");
                    if (cli_rc != 0 || cli_out.find("Status:         AVAILABLE") == std::string::npos) {
                        cleanup_peers();
                        return {false, "rfe.exe CLI upload failed (rc=" + std::to_string(cli_rc) + "): " + cli_out};
                    }
                }
            }

            // 6. Verify insufficient storage rejection during upload
            {
                json::Json storage_opts = json::Json::object();
                storage_opts["override_min_free_bytes"] = static_cast<int64_t>(900000000000000000LL); // ~900 PB

                json::Json storage_resp;
                if (!send_upload_request(5901, text_path, "F_UP_NO_SPACE", storage_opts, storage_resp) ||
                    storage_resp["status"].as_string() != "FAILED") {
                    cleanup_peers();
                    return {false, "upload with insufficient storage requirement should have been rejected"};
                }
                DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                FileManifest fm_nospace;
                if (db_a.get_file_manifest("F_UP_NO_SPACE", fm_nospace) && fm_nospace.status == "AVAILABLE") {
                    cleanup_peers();
                    return {false, "insufficient-storage failed upload was erroneously marked AVAILABLE"};
                }
            }

            // 7. Verify upload failure when fewer than 3 peers are online, and verify incomplete uploads
            //    are never marked AVAILABLE
            {
                std::string term_err;
                proc::terminate_peer(pids[2], term_err); // Stop P2P_C
                pids[2] = 0;
                proc::terminate_peer(pids[3], term_err); // Stop P2P_D
                pids[3] = 0;

                // Wait for P2P_A to detect P2P_C and P2P_D as OFFLINE (leaving only 2 online peers: A and B)
                const auto off_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
                while (std::chrono::steady_clock::now() < off_deadline) {
                    DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                    PeerInfo pc, pd;
                    if (db_a.get_peer("P2P_C", pc) && pc.state == PeerHealth::OFFLINE &&
                        db_a.get_peer("P2P_D", pd) && pd.state == PeerHealth::OFFLINE) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(150));
                }

                json::Json fail_resp;
                if (!send_upload_request(5901, text_path, "F_UP_TOO_FEW_PEERS", json::Json::object(), fail_resp) ||
                    fail_resp["status"].as_string() != "FAILED") {
                    cleanup_peers();
                    return {false, "upload with only 2 online peers should have failed clearly"};
                }

                DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                FileManifest fm_inc;
                if (db_a.get_file_manifest("F_UP_TOO_FEW_PEERS", fm_inc) && fm_inc.status == "AVAILABLE") {
                    cleanup_peers();
                    return {false, "incomplete upload with <3 online peers was erroneously marked AVAILABLE"};
                }
            }

            cleanup_peers();
            fs::remove_all(upload_dir, ec);
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "decentralized P2P upload verified: text/PDF/binary/padded files, RS(3,2), 3 distinct peers/stripe, SHA-256, fault retries & <3 peer rejection"};
        };

        auto t14_p2p_file_download = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const uint16_t discovery_port = 6005;
            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_A", 6001},
                {"P2P_B", 6002},
                {"P2P_C", 6003},
                {"P2P_D", 6004}
            };

            std::error_code ec;
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            const std::string work_dir = "output/t14_download_test";
            fs::remove_all(work_dir, ec);
            fs::create_directories(work_dir + "/in", ec);
            fs::create_directories(work_dir + "/out", ec);

            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid != 0) {
                        std::string term_err;
                        proc::terminate_peer(pid, term_err);
                    }
                }
                pids.clear();
            };

            // 1. Launch 4 decentralized peers (P2P_A..P2P_D) with zero coordinator involvement
            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch decentralized peer " + pp.first + ": " + err};
                }
                pids.push_back(pid);
            }

            auto query_peer_mesh = [](uint16_t port, json::Json& resp_out) -> bool {
                TCPClient client(1500, 1500);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_QUERY_PEERS;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_QUERY_PEERS_ACK;
            };

            auto send_upload_request = [](uint16_t port,
                                          const std::string& file_path,
                                          const std::string& file_id_override,
                                          json::Json& resp_out) -> bool {
                TCPClient client(3000, 20000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_UPLOAD_FILE;
                req["file_path"] = fs::absolute(file_path).string();
                req["filename"] = fs::path(file_path).filename().string();
                if (!file_id_override.empty()) req["file_id"] = file_id_override;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_UPLOAD_FILE_ACK &&
                       resp_out["status"].as_string() == "OK";
            };

            // Wait for all 4 peers to discover each other and mark all 4 ONLINE
            bool all_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int ready_count = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++ready_count;
                    }
                }
                if (ready_count == 4) {
                    all_discovered = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!all_discovered) {
                cleanup_peers();
                return {false, "4 decentralized peers did not complete discovery in T14"};
            }

            // Prepare 3 distinct binary formats:
            //   (1) Text file (flight_plan.txt, ~28 KB)
            //   (2) PDF file with binary stream (architecture_manual.pdf, ~160 KB, 2 stripes)
            //   (3) Arbitrary binary file with final-stripe padding (telemetry_padded.bin, 189,777 bytes, 2 stripes)
            const std::string text_in = work_dir + "/in/flight_plan.txt";
            {
                std::ofstream out(text_in, std::ios::binary | std::ios::trunc);
                for (int i = 0; i < 320; ++i) {
                    out << "Waypoint " << i << ": Decentralized P2P Search, Download & RS(3,2) Reconstruction Test.\r\n";
                }
            }
            const std::string pdf_in = work_dir + "/in/architecture_manual.pdf";
            {
                std::ofstream out(pdf_in, std::ios::binary | std::ios::trunc);
                const std::string hdr = "%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nstream\n";
                out.write(hdr.data(), static_cast<std::streamsize>(hdr.size()));
                std::vector<uint8_t> body(160000);
                for (size_t i = 0; i < body.size(); ++i) body[i] = static_cast<uint8_t>((i * 59 + 11) & 0xFF);
                out.write(reinterpret_cast<const char*>(body.data()), static_cast<std::streamsize>(body.size()));
                const std::string trl = "\nendstream\nendobj\n%%EOF\n";
                out.write(trl.data(), static_cast<std::streamsize>(trl.size()));
            }
            const std::string padded_in = work_dir + "/in/telemetry_padded.bin";
            {
                std::vector<uint8_t> arb(189777); // 2 stripes: S000 full 131072B, S001 padded (58705B -> D0=58705, D1=0)
                for (size_t i = 0; i < arb.size(); ++i) {
                    arb[i] = static_cast<uint8_t>((i & 0xFF) ^ ((i >> 7) * 97));
                }
                for (size_t b = 0; b < 256; ++b) arb[b] = static_cast<uint8_t>(b);
                std::ofstream out(padded_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(arb.data()), static_cast<std::streamsize>(arb.size()));
            }

            // 2. Upload all 3 files through Peer A (6001)
            json::Json up_text, up_pdf, up_pad;
            if (!send_upload_request(6001, text_in, "F_DL_TEXT", up_text) ||
                !send_upload_request(6001, pdf_in, "F_DL_PDF", up_pdf) ||
                !send_upload_request(6001, padded_in, "F_DL_PAD", up_pad)) {
                cleanup_peers();
                return {false, "failed to upload test files through Peer A in T14"};
            }

            // 3. Search for the uploaded files through Peer D (6004) via both TCP and rfe.exe CLI
            {
                TCPClient client(3000, 3000);
                if (!client.connect_to("127.0.0.1", 6004)) {
                    cleanup_peers();
                    return {false, "could not connect to Peer D (6004) for search"};
                }
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_CATALOGUE_QUERY;
                req["query"] = "telemetry_padded.bin";
                req["sync_if_missing"] = true;
                json::Json resp;
                std::vector<uint8_t> pl;
                if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
                    !resp.contains("files") || resp["files"].size() != 1 ||
                    resp["files"][0]["file_id"].as_string() != "F_DL_PAD" ||
                    !resp["files"][0]["reconstructable"].as_bool()) {
                    cleanup_peers();
                    return {false, "catalogue search for 'telemetry_padded.bin' on Peer D failed"};
                }
            }

            // 4. Download all 3 formats through Peer D (6004) while all 4 peers are online,
            //    verifying byte-for-byte equality, full-file SHA-256, and final stripe padding removal
            struct DlCase {
                std::string query;
                std::string orig_path;
                std::string out_path;
            };
            const std::vector<DlCase> dl_cases = {
                {"F_DL_TEXT", text_in,   work_dir + "/out/flight_plan_from_D.txt"},
                {"F_DL_PDF",  pdf_in,    work_dir + "/out/architecture_manual_from_D.pdf"},
                {"telemetry_padded.bin", padded_in, work_dir + "/out/telemetry_padded_from_D.bin"}
            };

            for (const auto& dc : dl_cases) {
                ClientDownload dl("127.0.0.1", 6004, work_dir + "/out");
                const DownloadResult res = dl.download_p2p(dc.query, dc.out_path);
                if (!res.success) {
                    cleanup_peers();
                    return {false, "download of " + dc.query + " via Peer D failed: " + res.message};
                }
                const std::string orig_sha = ChunkManager::sha256_of_file(dc.orig_path);
                if (res.actual_sha256 != orig_sha || !identical(dc.orig_path, dc.out_path)) {
                    cleanup_peers();
                    return {false, "downloaded file " + dc.out_path + " is not byte-identical to original"};
                }
            }

            // 5. Verify actual CLI commands (`rfe search` and `rfe download`) through Peer D (6004)
            const std::string rfe_exe = (fs::path(exe).parent_path() / "rfe.exe").string();
            if (fs::exists(rfe_exe, ec)) {
                const std::string search_cmd = "\"\"" + rfe_exe + "\" search architecture_manual.pdf --peer 127.0.0.1:6004 > \"" +
                                               work_dir + "/cli_search.txt\" 2>&1\"";
                const int s_rc = std::system(search_cmd.c_str());
                const std::string s_out = slurp(work_dir + "/cli_search.txt");
                if (s_rc != 0 || s_out.find("F_DL_PDF") == std::string::npos ||
                    s_out.find("Storage Peers:") == std::string::npos) {
                    cleanup_peers();
                    return {false, "rfe.exe search CLI command failed on Peer D: " + s_out};
                }

                const std::string cli_dl_out = work_dir + "/out/cli_downloaded_pdf.pdf";
                const std::string dl_cmd = "\"\"" + rfe_exe + "\" download F_DL_PDF --peer 127.0.0.1:6004 --output \"" +
                                           fs::absolute(cli_dl_out).string() + "\" > \"" +
                                           work_dir + "/cli_dl.txt\" 2>&1\"";
                const int d_rc = std::system(dl_cmd.c_str());
                const std::string d_out = slurp(work_dir + "/cli_dl.txt");
                if (d_rc != 0 || d_out.find("DECENTRALIZED DOWNLOAD COMPLETE") == std::string::npos ||
                    !identical(pdf_in, cli_dl_out)) {
                    cleanup_peers();
                    return {false, "rfe.exe download CLI command failed on Peer D: " + d_out};
                }
            }

            // 6. Stop Peer A (6001) — the ORIGINAL UPLOADER and a storage peer!
            //    Verify that the original uploader can remain OFFLINE and Peer D (6004) still
            //    reconstructs F_DL_PAD using the remaining 2 valid shards per stripe on B, C, D.
            {
                std::string term_err;
                proc::terminate_peer(pids[0], term_err); // Stop P2P_A
                pids[0] = 0;

                const std::string degraded_out = work_dir + "/out/padded_without_uploader_A.bin";
                ClientDownload dl("127.0.0.1", 6004, work_dir + "/out");
                const DownloadResult res = dl.download_p2p("F_DL_PAD", degraded_out);
                if (!res.success || !identical(padded_in, degraded_out) ||
                    res.actual_sha256 != ChunkManager::sha256_of_file(padded_in)) {
                    cleanup_peers();
                    return {false, "download failed when original uploader P2P_A was offline: " + res.message};
                }
            }

            // Restart P2P_A (6001) so all 3 storage peers for F_DL_PAD are online again for the corruption test
            {
                long new_a_pid = 0;
                std::string err;
                if (!proc::start_peer(exe, "P2P_A", "127.0.0.1", 6001, new_a_pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to restart P2P_A in T14: " + err};
                }
                pids[0] = new_a_pid;
                std::this_thread::sleep_for(std::chrono::milliseconds(600));
            }

            // 7. Corrupt one shard of F_DL_PAD on disk (e.g. F_DL_PAD-S000-D0 on whichever peer holds it)
            //    and verify that:
            //      (a) the corrupted shard is rejected by SHA-256 verification (`corrupted_shards_rejected >= 1`)
            //      (b) the downloader automatically fetches an alternative shard (`P0`) from another peer
            //      (c) RS(3,2) reconstructs `D0` from `D1 + P0` and produces a byte-identical file!
            std::string corrupted_shard_path;
            std::string original_shard_bytes;
            {
                DatabaseManager db_d("storage/peer_P2P_D/metadata.db");
                FileManifest fm;
                if (!db_d.get_file_manifest("F_DL_PAD", fm) || fm.shards.empty()) {
                    cleanup_peers();
                    return {false, "Peer D missing F_DL_PAD manifest"};
                }
                for (const auto& sh : fm.shards) {
                    if (sh.stripe_index == 0 && sh.shard_index == 0 && !sh.locations.empty()) {
                        const std::string host_peer = sh.locations[0].peer_id;
                        corrupted_shard_path = "storage/peer_" + host_peer + "/chunks/" + sh.shard_id + ".chunk";
                        original_shard_bytes = slurp(corrupted_shard_path);
                        std::fstream f(corrupted_shard_path, std::ios::in | std::ios::out | std::ios::binary);
                        if (!f) {
                            cleanup_peers();
                            return {false, "cannot open shard file to inject corruption: " + corrupted_shard_path};
                        }
                        f.write("CORRUPT_SHARD_BYTES_T14", 23);
                        break;
                    }
                }
                if (corrupted_shard_path.empty()) {
                    cleanup_peers();
                    return {false, "could not locate S000-D0 shard of F_DL_PAD to corrupt"};
                }

                const std::string corrupt_rec_out = work_dir + "/out/padded_after_corruption.bin";
                ClientDownload dl("127.0.0.1", 6004, work_dir + "/out");
                const DownloadResult res = dl.download_p2p("F_DL_PAD", corrupt_rec_out);
                if (!res.success || res.corrupted_shards_rejected < 1 || !identical(padded_in, corrupt_rec_out)) {
                    cleanup_peers();
                    return {false, "download did not reject corrupted shard and recover from alternate source: " + res.message};
                }
            }

            // 8. Test insufficient valid shards (< 2 valid shards for a stripe):
            //    With S000-D0 still corrupted, stop P2P_B and P2P_C so fewer than 2 valid shards remain.
            //    Verify that:
            //      (a) Download reports UNRECOVERABLE (`success == false`)
            //      (b) Failed download NEVER publishes a partial output file at the destination path!
            {
                std::string term_err;
                proc::terminate_peer(pids[1], term_err); // Stop P2P_B
                pids[1] = 0;
                proc::terminate_peer(pids[2], term_err); // Stop P2P_C
                pids[2] = 0;

                const std::string unrec_out = work_dir + "/out/should_not_exist_unrecoverable.bin";
                ClientDownload dl("127.0.0.1", 6004, work_dir + "/out");
                const DownloadResult res = dl.download_p2p("F_DL_PAD", unrec_out);
                if (res.success || res.message.find("UNRECOVERABLE") == std::string::npos) {
                    cleanup_peers();
                    return {false, "expected UNRECOVERABLE when fewer than 2 valid shards are available"};
                }
                if (fs::exists(unrec_out, ec)) {
                    cleanup_peers();
                    return {false, "failed download published a partial output file at " + unrec_out};
                }
            }

            cleanup_peers();
            fs::remove_all(work_dir, ec);
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "decentralized P2P search, download & RS(3,2) reconstruction verified: any-peer search, offline uploader, corrupted shard rejection, padded stripes & <2 shard refusal"};
        };

        auto t15_autonomous_recovery = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const uint16_t discovery_port = 6105;
            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_A", 6101},
                {"P2P_B", 6102},
                {"P2P_C", 6103},
                {"P2P_D", 6104}
            };

            std::error_code ec;
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            const std::string work_dir = "output/t15_recovery_test";
            fs::remove_all(work_dir, ec);
            fs::create_directories(work_dir + "/in", ec);
            fs::create_directories(work_dir + "/out", ec);

            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid != 0) {
                        std::string term_err;
                        proc::terminate_peer(pid, term_err);
                    }
                }
                pids.clear();
            };

            // 1. Launch 4 decentralized peers (P2P_A..P2P_D) without any central coordinator
            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch decentralized peer " + pp.first + ": " + err};
                }
                pids.push_back(pid);
            }

            auto query_peer_mesh = [](uint16_t port, json::Json& resp_out) -> bool {
                TCPClient client(1500, 1500);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_QUERY_PEERS;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_QUERY_PEERS_ACK;
            };

            auto send_upload_request = [](uint16_t port,
                                          const std::string& file_path,
                                          const std::string& file_id_override,
                                          json::Json& resp_out) -> bool {
                TCPClient client(3000, 20000);
                if (!client.connect_to("127.0.0.1", port)) return false;
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_UPLOAD_FILE;
                req["file_path"] = fs::absolute(file_path).string();
                req["filename"] = fs::path(file_path).filename().string();
                if (!file_id_override.empty()) req["file_id"] = file_id_override;
                std::vector<uint8_t> pl;
                return client.request(req, {}, resp_out, pl) == proto::RecvStatus::OK &&
                       resp_out["type"].as_string() == proto::P2P_UPLOAD_FILE_ACK &&
                       resp_out["status"].as_string() == "OK";
            };

            // Wait for all 4 peers to discover each other and mark all 4 ONLINE
            bool all_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int ready_count = 0;
                for (const auto& pp : p2p_peers) {
                    json::Json resp;
                    if (query_peer_mesh(pp.second, resp) &&
                        resp.contains("peers") && resp["peers"].is_array() &&
                        resp["peers"].size() == 4) {
                        int online_cnt = 0;
                        for (const auto& item : resp["peers"].items()) {
                            if (item["health"].as_string() == "ONLINE") ++online_cnt;
                        }
                        if (online_cnt == 4) ++ready_count;
                    }
                }
                if (ready_count == 4) {
                    all_discovered = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!all_discovered) {
                cleanup_peers();
                return {false, "4 decentralized peers did not complete discovery in T15"};
            }

            // 2. Upload a 2-stripe binary file (F_REC_001, 165,000 bytes -> 6 shards across A, B, C, D)
            const std::string rec_in = work_dir + "/in/autonomous_payload.bin";
            {
                std::vector<uint8_t> data(165000);
                for (size_t i = 0; i < data.size(); ++i) {
                    data[i] = static_cast<uint8_t>((i * 89 + 31) & 0xFF);
                }
                std::ofstream out(rec_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
            }

            json::Json up_resp;
            if (!send_upload_request(6101, rec_in, "F_REC_001", up_resp)) {
                cleanup_peers();
                return {false, "failed to upload F_REC_001 in T15"};
            }

            // Wait for initial manifest (version 2) to sync across all 4 peers
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            // Inspect initial shard locations for F_REC_001 and verify 3 distinct peers per stripe
            FileManifest initial_m;
            {
                DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                if (!db_a.get_file_manifest("F_REC_001", initial_m) || initial_m.shards.size() != 6) {
                    cleanup_peers();
                    return {false, "initial manifest F_REC_001 missing or incomplete on P2P_A"};
                }
            }

            // Identify which shards are hosted on P2P_B (which we will stop)
            std::vector<ShardManifestEntry> b_hosted_shards;
            for (const auto& sh : initial_m.shards) {
                for (const auto& loc : sh.locations) {
                    if (loc.peer_id == "P2P_B" && loc.location_status == "STORED") {
                        b_hosted_shards.push_back(sh);
                    }
                }
            }
            if (b_hosted_shards.empty()) {
                cleanup_peers();
                return {false, "expected P2P_B to host at least one shard of F_REC_001"};
            }

            // 3. Verify Task 5.1 & 5.3:
            //    - Pre-plant an active recovery lease owned by P2P_C that is about to expire,
            //      plus test duplicate lease request rejection while lease is active.
            {
                const auto& first_b_sh = b_hosted_shards[0];
                std::ostringstream r_ss;
                r_ss << "REC:F_REC_001:S" << std::setfill('0') << std::setw(3)
                     << first_b_sh.stripe_index << ":" << first_b_sh.shard_id;
                const std::string rec_id = r_ss.str();

                TCPClient lclient(1500, 1500);
                if (!lclient.connect_to("127.0.0.1", 6101)) {
                    cleanup_peers();
                    return {false, "could not connect to P2P_A for lease test"};
                }
                json::Json lreq = json::Json::object();
                lreq["type"] = proto::P2P_RECOVERY_LEASE_REQ;
                lreq["recovery_id"] = rec_id;
                lreq["failed_peer_id"] = "P2P_B";
                lreq["shard_id"] = first_b_sh.shard_id;
                lreq["file_id"] = "F_REC_001";
                lreq["stripe_index"] = static_cast<long long>(first_b_sh.stripe_index);
                lreq["rebuilder_peer_id"] = "P2P_GHOST_OWNER";

                json::Json lresp;
                std::vector<uint8_t> pl;
                if (lclient.request(lreq, {}, lresp, pl) != proto::RecvStatus::OK ||
                    !lresp["granted"].as_bool()) {
                    cleanup_peers();
                    return {false, "initial recovery lease request should have been granted"};
                }

                // Immediate competing lease request from another peer must be rejected while lease is active
                TCPClient lclient2(1500, 1500);
                lclient2.connect_to("127.0.0.1", 6101);
                lreq["rebuilder_peer_id"] = "P2P_COMPETING_PEER";
                json::Json lresp2;
                if (lclient2.request(lreq, {}, lresp2, pl) != proto::RecvStatus::OK ||
                    lresp2["granted"].as_bool()) {
                    cleanup_peers();
                    return {false, "competing recovery lease request should have been rejected while active"};
                }

                // Backdate the ghost owner's lease timestamp in P2P_A's DB so it expires immediately
                // (simulating recovery owner stopping unexpectedly -> surviving peer takes over!)
                DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                RecoveryRecordEntry ghost_rec;
                if (db_a.get_recovery_record(rec_id, ghost_rec)) {
                    ghost_rec.timestamp_epoch_ms -= 10000;
                    db_a.upsert_recovery_record(ghost_rec);
                }
            }

            // 4. Stop P2P_B (6102) — NO manual repair command is issued!
            //    Wait for surviving peers (P2P_A, P2P_C, P2P_D) to:
            //      - Detect P2P_B as OFFLINE (only after SUSPECT -> OFFLINE threshold, not on SUSPECT)
            //      - Identify affected stripes of F_REC_001
            //      - Retrieve the 2 surviving shards per affected stripe
            //      - Reconstruct the missing shard via RS(3,2) & verify its SHA-256
            //      - Select the sole remaining online peer not in that stripe as replacement peer
            //      - Store the reconstructed shard on the replacement peer & update SQLite
            //      - Synchronize the updated manifest & COMPLETED recovery_records across A, C, D
            {
                std::string term_err;
                proc::terminate_peer(pids[1], term_err); // Stop P2P_B
                pids[1] = 0;
            }

            bool autonomous_repair_complete = false;
            const auto repair_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
            while (std::chrono::steady_clock::now() < repair_deadline) {
                int peers_with_full_repair = 0;
                for (size_t pi : {size_t(0), size_t(2), size_t(3)}) { // P2P_A, P2P_C, P2P_D
                    const std::string& pid = p2p_peers[pi].first;
                    DatabaseManager db("storage/peer_" + pid + "/metadata.db");
                    FileManifest fm;
                    if (!db.get_file_manifest("F_REC_001", fm) || fm.version <= 2) continue;

                    // Verify every shard that was on P2P_B now has a STORED location on one of A, C, D
                    // AND the physical .chunk file exists on that replacement peer's disk with exact SHA-256!
                    bool all_b_shards_repaired = true;
                    for (const auto& b_sh : b_hosted_shards) {
                        bool this_shard_ok = false;
                        for (const auto& cur_sh : fm.shards) {
                            if (cur_sh.shard_id != b_sh.shard_id) continue;
                            std::set<std::string> stripe_peers;
                            for (const auto& loc : cur_sh.locations) {
                                if (loc.location_status == "STORED" && loc.peer_id != "P2P_B") {
                                    const std::string disk_p =
                                        "storage/peer_" + loc.peer_id + "/chunks/" + cur_sh.shard_id + ".chunk";
                                    if (fs::exists(disk_p, ec) &&
                                        fs::file_size(disk_p, ec) == 65536 &&
                                        ChunkManager::sha256_of_file(disk_p) == b_sh.sha256) {
                                        this_shard_ok = true;
                                    }
                                }
                            }
                        }
                        if (!this_shard_ok) {
                            all_b_shards_repaired = false;
                            break;
                        }
                    }

                    // Also verify 3 distinct STORED online peers per stripe (A, C, D)
                    for (int32_t st_i = 0; st_i < 2; ++st_i) {
                        std::set<std::string> distinct_online;
                        for (const auto& sh : fm.shards) {
                            if (sh.stripe_index != st_i) continue;
                            for (const auto& loc : sh.locations) {
                                if (loc.location_status == "STORED" && loc.peer_id != "P2P_B") {
                                    distinct_online.insert(loc.peer_id);
                                }
                            }
                        }
                        if (distinct_online.size() != 3) {
                            all_b_shards_repaired = false;
                        }
                    }

                    if (all_b_shards_repaired) ++peers_with_full_repair;
                }
                if (peers_with_full_repair == 3) {
                    autonomous_repair_complete = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }

            if (!autonomous_repair_complete) {
                cleanup_peers();
                return {false, "autonomous background recovery did not rebuild and synchronize P2P_B's shards across A, C, D"};
            }

            // 5. Confirm F_REC_001 remains downloadable and byte-identical through Peer D (6104)
            {
                const std::string dl_out = work_dir + "/out/recovered_after_auto_repair.bin";
                ClientDownload dl("127.0.0.1", 6104, work_dir + "/out");
                const DownloadResult res = dl.download_p2p("F_REC_001", dl_out);
                if (!res.success || !identical(rec_in, dl_out)) {
                    cleanup_peers();
                    return {false, "download of F_REC_001 after autonomous repair failed: " + res.message};
                }
            }

            // 6. Task 5.8: Restart failed peer P2P_B (6102) and verify:
            //    - P2P_B synchronizes to the newer manifest version
            //    - P2P_B's old replaced shard locations remain STALE (no duplicate STORED locations)
            //    - P2P_B cleans up its stale on-disk .chunk copies for shards that were replaced while it was offline
            //    - No duplicate or inconsistent repair occurs after P2P_B rejoins
            {
                long new_b_pid = 0;
                std::string err;
                if (!proc::start_peer(exe, "P2P_B", "127.0.0.1", 6102, new_b_pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to restart P2P_B in T15: " + err};
                }
                pids[1] = new_b_pid;
            }

            bool b_reconciled = false;
            const auto rejoin_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < rejoin_deadline) {
                DatabaseManager db_b("storage/peer_P2P_B/metadata.db");
                FileManifest fm_b;
                if (db_b.get_file_manifest("F_REC_001", fm_b) && fm_b.version > 2) {
                    bool stale_cleaned = true;
                    for (const auto& b_sh : b_hosted_shards) {
                        const std::string old_disk = "storage/peer_P2P_B/chunks/" + b_sh.shard_id + ".chunk";
                        if (fs::exists(old_disk, ec)) {
                            stale_cleaned = false;
                        }
                        for (const auto& cur_sh : fm_b.shards) {
                            if (cur_sh.shard_id == b_sh.shard_id) {
                                int stored_cnt = 0;
                                for (const auto& loc : cur_sh.locations) {
                                    if (loc.location_status == "STORED") ++stored_cnt;
                                    if (loc.peer_id == "P2P_B" && loc.location_status != "STALE") {
                                        stale_cleaned = false;
                                    }
                                }
                                if (stored_cnt != 1) stale_cleaned = false;
                            }
                        }
                    }
                    if (stale_cleaned) {
                        b_reconciled = true;
                        break;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!b_reconciled) {
                cleanup_peers();
                return {false, "restarted peer P2P_B did not mark its replaced shards STALE and remove duplicate .chunk files"};
            }

            // 7. Test edge cases via P2P_REPAIR_TRIGGER:
            //    (a) Replacement peer running out of storage (`override_min_free_bytes` -> FAILED recorded)
            //    (b) Recovery interrupted during transfer (`simulate_interrupted_store` -> retried & COMPLETED)
            //    (c) Corrupted surviving shard + missing shard (<2 valid shards -> UNRECOVERABLE)
            //    (d) Two missing shards in the same stripe -> UNRECOVERABLE
            {
                // Upload a second file F_REC_EDGE (1 stripe = 3 shards)
                const std::string edge_in = work_dir + "/in/edge_case.bin";
                {
                    std::vector<uint8_t> ed(70000, 0x5A);
                    std::ofstream out(edge_in, std::ios::binary | std::ios::trunc);
                    out.write(reinterpret_cast<const char*>(ed.data()), static_cast<std::streamsize>(ed.size()));
                }
                json::Json edge_up;
                if (!send_upload_request(6101, edge_in, "F_REC_EDGE", edge_up)) {
                    cleanup_peers();
                    return {false, "failed to upload F_REC_EDGE"};
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(400));

                // Corrupt S000-D0 on whichever peer holds it, then trigger repair with insufficient storage first,
                // then trigger repair with simulated interrupted store (which retries and succeeds!)
                DatabaseManager db_a("storage/peer_P2P_A/metadata.db");
                FileManifest fm_edge;
                db_a.get_file_manifest("F_REC_EDGE", fm_edge);
                std::string d0_host;
                std::string d0_id;
                uint16_t d0_port = 6101;
                for (const auto& sh : fm_edge.shards) {
                    if (sh.stripe_index == 0 && sh.shard_index == 0 && !sh.locations.empty()) {
                        d0_host = sh.locations[0].peer_id;
                        d0_id = sh.shard_id;
                        for (const auto& pp : p2p_peers) {
                            if (pp.first == d0_host) d0_port = pp.second;
                        }
                        break;
                    }
                }

                // Corrupt d0_id on d0_host so d0_host detects local corruption and repairs it onto the 4th peer
                const std::string d0_path = "storage/peer_" + d0_host + "/chunks/" + d0_id + ".chunk";
                {
                    std::fstream f(d0_path, std::ios::in | std::ios::out | std::ios::binary);
                    f.write("CORRUPT_EDGE_D0", 15);
                }

                // Trigger repair on d0_port with impossible free-space requirement (900 PB) -> must record FAILED
                {
                    TCPClient c(3000, 5000);
                    c.connect_to("127.0.0.1", d0_port);
                    json::Json req = json::Json::object();
                    req["type"] = proto::P2P_REPAIR_TRIGGER;
                    req["force_owner"] = true;
                    req["override_min_free_bytes"] = static_cast<int64_t>(900000000000000000LL);
                    json::Json resp;
                    std::vector<uint8_t> pl;
                    c.request(req, {}, resp, pl);
                }

                // Now trigger repair on d0_port with simulate_interrupted_store = true -> retries & succeeds!
                {
                    TCPClient c(3000, 5000);
                    c.connect_to("127.0.0.1", d0_port);
                    json::Json req = json::Json::object();
                    req["type"] = proto::P2P_REPAIR_TRIGGER;
                    req["force_owner"] = true;
                    req["simulate_interrupted_store"] = true;
                    json::Json resp;
                    std::vector<uint8_t> pl;
                    if (c.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
                        resp["repaired_count"].as_int() < 1) {
                        cleanup_peers();
                        return {false, "repair after simulated interrupted transfer did not succeed on retry"};
                    }
                }

                // Finally, test >=2 missing shards in the same stripe -> UNRECOVERABLE
                {
                    std::string term_err;
                    proc::terminate_peer(pids[1], term_err); // Stop P2P_B
                    pids[1] = 0;
                    proc::terminate_peer(pids[2], term_err); // Stop P2P_C
                    pids[2] = 0;
                    proc::terminate_peer(pids[3], term_err); // Stop P2P_D
                    pids[3] = 0;

                    const auto off_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
                    while (std::chrono::steady_clock::now() < off_deadline) {
                        DatabaseManager db_check("storage/peer_P2P_A/metadata.db");
                        PeerInfo pb, pc, pd;
                        if (db_check.get_peer("P2P_B", pb) && pb.state == PeerHealth::OFFLINE &&
                            db_check.get_peer("P2P_C", pc) && pc.state == PeerHealth::OFFLINE &&
                            db_check.get_peer("P2P_D", pd) && pd.state == PeerHealth::OFFLINE) {
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(150));
                    }

                    // Trigger repair scan on P2P_A (6101) when 2 peers (C and D) are offline -> must mark F_REC_001 UNRECOVERABLE
                    TCPClient c(3000, 5000);
                    c.connect_to("127.0.0.1", 6101);
                    json::Json req = json::Json::object();
                    req["type"] = proto::P2P_REPAIR_TRIGGER;
                    req["force_owner"] = true;
                    json::Json resp;
                    std::vector<uint8_t> pl;
                    c.request(req, {}, resp, pl);

                    DatabaseManager db_check("storage/peer_P2P_A/metadata.db");
                    FileManifest fm_unrec;
                    if (!db_check.get_file_manifest("F_REC_001", fm_unrec) ||
                        fm_unrec.status != "UNRECOVERABLE") {
                        cleanup_peers();
                        return {false, "expected F_REC_001 status=UNRECOVERABLE when 2 shards in a stripe are offline"};
                    }
                }
            }

            cleanup_peers();
            fs::remove_all(work_dir, ec);
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "autonomous P2P recovery verified: OFFLINE detection, lease coordination, RS(3,2) rebuild, 4th-peer placement, rejoin stale cleanup & <2 shard UNRECOVERABLE"};
        };

        // -------------------------------------------------------------------------
        // T16_WEB_DASHBOARD_E2E: Embedded C++ HTTP server, static UI assets, REST API v1,
        // CSRF protection, file upload via web API, P2P distribution, search, download streaming
        // -------------------------------------------------------------------------
        auto t16_web_dashboard_e2e = [&]() -> Result {
            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer executable not found"};

            const std::string work_dir = "test_web_dashboard";
            std::error_code ec;
            fs::remove_all(work_dir, ec);
            fs::create_directories(work_dir + "/in", ec);
            fs::create_directories(work_dir + "/out", ec);

            const std::vector<std::pair<std::string, uint16_t>> p2p_peers = {
                {"P2P_W1", 6301}, {"P2P_W2", 6302}, {"P2P_W3", 6303}
            };
            const uint16_t discovery_port = 6300;

            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }

            std::vector<long> pids;
            auto cleanup_peers = [&]() {
                for (long pid : pids) {
                    if (pid != 0) {
                        std::string term_err;
                        proc::terminate_peer(pid, term_err);
                    }
                }
                pids.clear();
            };

            // 1. Launch 3 peers with embedded Web Dashboard HTTP servers
            for (const auto& pp : p2p_peers) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pp.first, "127.0.0.1", pp.second, pid, err, "p2p", discovery_port)) {
                    cleanup_peers();
                    return {false, "failed to launch peer " + pp.first + " for web dashboard test: " + err};
                }
                pids.push_back(pid);
            }

            // Helper for HTTP GET
            auto http_get = [](uint16_t port, const std::string& path, int& status_code_out,
                               std::string& body_out, const std::vector<std::string>& extra_headers = {}) -> bool {
                socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (s == INVALID_SOCK) return false;
                proto::set_timeouts(s, 4000, 4000);
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
                if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                    ::closesocket(s);
                    return false;
                }
                std::ostringstream req;
                req << "GET " << path << " HTTP/1.1\r\n"
                    << "Host: 127.0.0.1:" << port << "\r\n"
                    << "Connection: close\r\n";
                for (const auto& h : extra_headers) req << h << "\r\n";
                req << "\r\n";
                const std::string r_str = req.str();
                if (!proto::send_all(s, r_str.data(), r_str.size())) {
                    ::closesocket(s);
                    return false;
                }
                std::string resp_data;
                char buf[8192];
                int n = 0;
                while ((n = ::recv(s, buf, sizeof(buf), 0)) > 0) {
                    resp_data.append(buf, static_cast<size_t>(n));
                }
                ::closesocket(s);

                const size_t header_end = resp_data.find("\r\n\r\n");
                if (header_end == std::string::npos) return false;
                const std::string header = resp_data.substr(0, header_end);
                body_out = resp_data.substr(header_end + 4);

                const size_t sp1 = header.find(' ');
                if (sp1 == std::string::npos) return false;
                const size_t sp2 = header.find(' ', sp1 + 1);
                if (sp2 == std::string::npos) return false;
                try {
                    status_code_out = std::stoi(header.substr(sp1 + 1, sp2 - sp1 - 1));
                    return true;
                } catch (...) {
                    return false;
                }
            };

            // Helper for HTTP POST
            auto http_post = [](uint16_t port, const std::string& path, const std::string& content_type,
                                const std::string& body, int& status_code_out, std::string& body_out,
                                const std::vector<std::string>& extra_headers = {}) -> bool {
                socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (s == INVALID_SOCK) return false;
                proto::set_timeouts(s, 6000, 6000);
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
                if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                    ::closesocket(s);
                    return false;
                }
                std::ostringstream req;
                req << "POST " << path << " HTTP/1.1\r\n"
                    << "Host: 127.0.0.1:" << port << "\r\n"
                    << "Content-Type: " << content_type << "\r\n"
                    << "Content-Length: " << body.size() << "\r\n"
                    << "Connection: close\r\n";
                for (const auto& h : extra_headers) req << h << "\r\n";
                req << "\r\n";
                req << body;
                const std::string r_str = req.str();
                if (!proto::send_all(s, r_str.data(), r_str.size())) {
                    ::closesocket(s);
                    return false;
                }
                std::string resp_data;
                char buf[8192];
                int n = 0;
                while ((n = ::recv(s, buf, sizeof(buf), 0)) > 0) {
                    resp_data.append(buf, static_cast<size_t>(n));
                }
                ::closesocket(s);

                const size_t header_end = resp_data.find("\r\n\r\n");
                if (header_end == std::string::npos) return false;
                const std::string header = resp_data.substr(0, header_end);
                body_out = resp_data.substr(header_end + 4);

                const size_t sp1 = header.find(' ');
                if (sp1 == std::string::npos) return false;
                const size_t sp2 = header.find(' ', sp1 + 1);
                if (sp2 == std::string::npos) return false;
                try {
                    status_code_out = std::stoi(header.substr(sp1 + 1, sp2 - sp1 - 1));
                    return true;
                } catch (...) {
                    return false;
                }
            };

            // Resolve bound UI port for peer 1 and peer 3
            auto get_bound_ui_port = [](const std::string& peer_name, uint16_t default_port) -> uint16_t {
                const std::string db_file = "storage/peer_" + peer_name + "/metadata.db";
                for (int attempt = 0; attempt < 50; ++attempt) {
                    if (fs::exists(db_file)) {
                        try {
                            DatabaseManager test_db(db_file);
                            const std::string val = test_db.get_config("ui_port", "");
                            if (!val.empty()) {
                                int p = std::stoi(val);
                                if (p > 0 && p <= 65535) return static_cast<uint16_t>(p);
                            }
                        } catch (...) {}
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                return default_port;
            };

            const uint16_t ui_port_w1 = get_bound_ui_port("P2P_W1", 8081);
            const uint16_t ui_port_w3 = get_bound_ui_port("P2P_W3", 8083);

            // Wait for Peer 1 Web Dashboard HTTP server to respond with 200 on /api/v1/health
            bool web_ready = false;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
            while (std::chrono::steady_clock::now() < deadline) {
                int status = 0;
                std::string body;
                if (http_get(ui_port_w1, "/api/v1/health", status, body) && status == 200) {
                    try {
                        auto j = json::Json::parse(body);
                        if (j.contains("status") && j["status"].as_string() == "OK" &&
                            j.contains("peer_id") && j["peer_id"].as_string() == "P2P_W1") {
                            web_ready = true;
                            break;
                        }
                    } catch (...) {}
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (!web_ready) {
                cleanup_peers();
                return {false, "Web Dashboard HTTP server did not become ready on port " + std::to_string(ui_port_w1)};
            }

            // Wait for all 3 peers to discover each other via UDP beacons and show ONLINE in P2P_W1's registry
            bool cluster_discovered = false;
            const auto disc_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
            while (std::chrono::steady_clock::now() < disc_deadline) {
                int st_p = 0;
                std::string body_p;
                if (http_get(ui_port_w1, "/api/v1/peers", st_p, body_p) && st_p == 200) {
                    try {
                        auto jp = json::Json::parse(body_p);
                        if (jp.contains("peers") && jp["peers"].is_array() && jp["peers"].size() >= 3) {
                            int online_cnt = 0;
                            for (const auto& pi : jp["peers"].items()) {
                                if (pi["health"].as_string() == "ONLINE") ++online_cnt;
                            }
                            if (online_cnt >= 3) {
                                cluster_discovered = true;
                                break;
                            }
                        }
                    } catch (...) {}
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
            }
            if (!cluster_discovered) {
                cleanup_peers();
                return {false, "Cluster peers did not finish mutual discovery (need 3 online peers for RS(3,2))"};
            }

            // 2. Verify Static Web Dashboard UI Assets
            {
                int st_index = 0, st_css = 0, st_js = 0;
                std::string body_index, body_css, body_js;
                if (!http_get(ui_port_w1, "/", st_index, body_index) || st_index != 200 ||
                    body_index.find("html") == std::string::npos ||
                    body_index.find("Peer") == std::string::npos) {
                    cleanup_peers();
                    return {false, "GET / failed or index.html missing: status=" + std::to_string(st_index)};
                }
                if (!http_get(ui_port_w1, "/styles.css", st_css, body_css) || st_css != 200 || body_css.size() < 100) {
                    cleanup_peers();
                    return {false, "GET /styles.css failed: status=" + std::to_string(st_css)};
                }
                if (!http_get(ui_port_w1, "/app.js", st_js, body_js) || st_js != 200 || body_js.size() < 100) {
                    cleanup_peers();
                    return {false, "GET /app.js failed: status=" + std::to_string(st_js)};
                }
            }

            // 3. Verify REST API v1 Core Endpoints
            {
                int st_node = 0, st_peers = 0, st_storage = 0;
                std::string b_node, b_peers, b_storage;
                if (!http_get(ui_port_w1, "/api/v1/node", st_node, b_node) || st_node != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/node failed: status=" + std::to_string(st_node)};
                }
                auto j_node = json::Json::parse(b_node);
                if (j_node["peer_id"].as_string() != "P2P_W1" || !j_node.contains("storage_dir")) {
                    cleanup_peers();
                    return {false, "GET /api/v1/node response invalid: " + b_node};
                }

                if (!http_get(ui_port_w1, "/api/v1/peers", st_peers, b_peers) || st_peers != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/peers failed: status=" + std::to_string(st_peers)};
                }
                auto j_peers = json::Json::parse(b_peers);
                if (!j_peers.contains("peers") || !j_peers["peers"].is_array()) {
                    cleanup_peers();
                    return {false, "GET /api/v1/peers response invalid: " + b_peers};
                }

                if (!http_get(ui_port_w1, "/api/v1/storage", st_storage, b_storage) || st_storage != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/storage failed: status=" + std::to_string(st_storage)};
                }
                auto j_storage = json::Json::parse(b_storage);
                if (!j_storage.contains("available_bytes") || !j_storage.contains("local_shards_count")) {
                    cleanup_peers();
                    return {false, "GET /api/v1/storage response invalid: " + b_storage};
                }
            }

            // 4. Verify CSRF Protection on POST Endpoints
            {
                int st_csrf_fail = 0, st_csrf_ok = 0;
                std::string b_csrf_fail, b_csrf_ok;
                // POST without X-RFE-Request header must fail with 403 Forbidden
                http_post(ui_port_w1, "/api/v1/settings", "application/json", "{\"max_storage_mb\":2048}",
                          st_csrf_fail, b_csrf_fail, {});
                if (st_csrf_fail != 403) {
                    cleanup_peers();
                    return {false, "expected HTTP 403 for POST without X-RFE-Request CSRF header, got " + std::to_string(st_csrf_fail)};
                }

                // POST with X-RFE-Request: 1 must succeed with 200 OK
                http_post(ui_port_w1, "/api/v1/settings", "application/json", "{\"max_storage_mb\":2048}",
                          st_csrf_ok, b_csrf_ok, {"X-RFE-Request: 1"});
                if (st_csrf_ok != 200) {
                    cleanup_peers();
                    return {false, "expected HTTP 200 for valid settings update, got " + std::to_string(st_csrf_ok)};
                }
            }

            // 5. Verify File Upload via Web HTTP API (multipart/form-data) on Peer 1
            const std::string test_payload = "E2E Web Dashboard Test Data: Distributed Reed-Solomon RS(3,2) chunking across peer mesh.\n"
                                             "Verifying HTTP upload, streaming download, SHA-256 integrity, and UI catalogue sync.\n";
            const std::string expected_sha = ChunkManager::sha256_of_bytes(
                std::vector<uint8_t>(test_payload.begin(), test_payload.end()));
            const std::string boundary = "----WebKitFormBoundaryE2eTest7MA4";

            std::ostringstream mp_body;
            mp_body << "--" << boundary << "\r\n"
                    << "Content-Disposition: form-data; name=\"file\"; filename=\"dashboard_doc.txt\"\r\n"
                    << "Content-Type: text/plain\r\n\r\n"
                    << test_payload << "\r\n"
                    << "--" << boundary << "--\r\n";
            const std::string mp_str = mp_body.str();

            int st_upload = 0;
            std::string b_upload;
            if (!http_post(ui_port_w1, "/api/v1/uploads",
                           "multipart/form-data; boundary=" + boundary, mp_str,
                           st_upload, b_upload, {"X-RFE-Request: 1"}) ||
                st_upload != 200) {
                cleanup_peers();
                return {false, "POST /api/v1/uploads failed: status=" + std::to_string(st_upload) + " body=" + b_upload};
            }
            auto j_up = json::Json::parse(b_upload);
            if (j_up["status"].as_string() != "OK" || !j_up.contains("file_id")) {
                cleanup_peers();
                return {false, "POST /api/v1/uploads returned error: " + b_upload};
            }
            const std::string uploaded_file_id = j_up["file_id"].as_string();

            // 6. Verify Catalogue Listing and Search via Web HTTP API
            {
                int st_files = 0, st_search = 0;
                std::string b_files, b_search;
                if (!http_get(ui_port_w1, "/api/v1/files", st_files, b_files) || st_files != 200 ||
                    b_files.find(uploaded_file_id) == std::string::npos) {
                    cleanup_peers();
                    return {false, "uploaded file_id not found in GET /api/v1/files: " + b_files};
                }
                if (!http_get(ui_port_w1, "/api/v1/search?q=dashboard_doc.txt", st_search, b_search) ||
                    st_search != 200 || b_search.find(uploaded_file_id) == std::string::npos) {
                    cleanup_peers();
                    return {false, "uploaded file not found in GET /api/v1/search: " + b_search};
                }
            }

            // 7. Verify File Download & Streaming via Web HTTP API on Peer 3 (ui_port_w3)
            {
                int st_dl = 0;
                std::string downloaded_body;
                if (!http_get(ui_port_w3, "/api/v1/downloads/" + uploaded_file_id, st_dl, downloaded_body) ||
                    st_dl != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/downloads/" + uploaded_file_id + " failed: status=" + std::to_string(st_dl)};
                }
                const std::string dl_sha = ChunkManager::sha256_of_bytes(
                    std::vector<uint8_t>(downloaded_body.begin(), downloaded_body.end()));
                if (dl_sha != expected_sha || downloaded_body != test_payload) {
                    cleanup_peers();
                    return {false, "downloaded file content/SHA mismatch: expected " + expected_sha + ", got " + dl_sha};
                }
            }

            // 8. Verify Telemetry & Recovery Event Logging via Web HTTP API
            {
                int st_trans = 0, st_events = 0;
                std::string b_trans, b_events;
                if (!http_get(ui_port_w1, "/api/v1/transfers", st_trans, b_trans) || st_trans != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/transfers failed: status=" + std::to_string(st_trans)};
                }
                if (!http_get(ui_port_w1, "/api/v1/events", st_events, b_events) || st_events != 200) {
                    cleanup_peers();
                    return {false, "GET /api/v1/events failed: status=" + std::to_string(st_events)};
                }
            }

            cleanup_peers();
            fs::remove_all(work_dir, ec);
            for (const auto& pp : p2p_peers) {
                fs::remove_all("storage/peer_" + pp.first, ec);
            }
            return {true, "Web Dashboard E2E verified: HTTP server, static UI assets, REST API v1, CSRF protection, file upload, search, download streaming & SHA-256 integrity"};
        };

        struct Test { const char* name; std::function<Result()> run; };
        const std::vector<Test> tests = {
            {"T01", t01}, {"T02", t02}, {"T03", t03}, {"T04", t04},
            {"T05", t05}, {"T06", t06}, {"T07", t07}, {"T08_M2", t08},
            {"T09_RS", t09_rs}, {"T10_GATE2", t10_gate2_e2e},
            {"T11_PEER_FOUNDATION", t11_peer_foundation},
            {"T12_DISTRIBUTED_METADATA", t12_distributed_metadata},
            {"T13_P2P_FILE_UPLOAD", t13_p2p_file_upload},
            {"T14_P2P_FILE_DOWNLOAD", t14_p2p_file_download},
            {"T15_AUTONOMOUS_RECOVERY", t15_autonomous_recovery},
            {"T16_WEB_DASHBOARD_E2E", t16_web_dashboard_e2e}};

        std::cout << "================ Gate-1 & Gate-2 test suite ================\n";
        int pass = 0, failn = 0;
        for (const auto& t : tests) {
            if (!only.empty() &&
                std::find(only.begin(), only.end(), std::string(t.name)) == only.end())
                continue;
            Result r;
            try {
                r = t.run();
            } catch (const std::exception& e) {
                r = {false, std::string("exception: ") + e.what()};
            }
            std::cout << (r.first ? "PASS " : "FAIL ") << t.name << " - " << r.second << std::endl;
            (r.first ? pass : failn)++;
        }
        quiet([&] { return cmd_sys_stop(); });
        quiet([&] { return cmd_reset(); });
        std::cout << "===================================================\n";
        std::cout << "PASS=" << pass << " FAIL=" << failn << "\n";
        return failn == 0 ? 0 : 1;
    }

private:
    json::Json config_;
    std::vector<PeerInfo> peers_;
    MetadataManager meta_{"metadata"};
    uint64_t chunk_size_ = 65536;
    int hb_interval_ = 500, hb_timeout_ = 1000, hb_threshold_ = 2;
    std::string coord_bind_address_ = "0.0.0.0";
    uint16_t coord_port_ = 5000;

    std::string staging_dir() const { return "output/staging"; }
    std::string output_dir() const { return "output"; }
};

static void usage() {
    std::cout <<
        "usage: controller <command> [args]\n"
        "  coordinator [port] [bind_ip] start Gate-2 dynamic registration listener\n"
        "  peers [coord_ip] [port]      query Coordinator and display live peer registry\n"
        "  sys start|stop|status        start / stop / list the 3 peer processes\n"
        "  fail <A|B|C|random>          simulate a peer process failure\n"
        "  demo [file]                  full Gate-1 demonstration (default: input/sample_file.bin)\n"
        "  test [T01 .. T08_M2]         run tests\n"
        "  chunk <file>                 E1: chunk file + SHA-256 + metadata\n"
        "  distribute                   E3: round-robin + replication to peers\n"
        "  ping                         E2: REGISTER handshake with all peers\n"
        "  detect [--all] [--duration-ms N]\n"
        "                               E4: heartbeat monitor (stops at first failure)\n"
        "  recover [--failed <id>] [--simulate-interrupt]\n"
        "                               E5: recover chunks from surviving replicas\n"
        "  reconstruct [original]       E5: order, concatenate, byte-compare\n"
        "  status                       probe and show peer states\n"
        "  reset                        clear metadata, peer storage, output\n";
}

int main(int argc, char** argv) {
    const std::vector<std::string> args = proc::command_line_args(argc, argv);
    if (args.size() < 2) {
        usage();
        return 1;
    }
    const std::string cmd = args[1];
    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        usage();
        return 0;
    }
    if (!sock_init()) {
        std::cerr << "controller error: socket subsystem initialisation failed\n";
        return 1;
    }
    try {
        std::string root, err;
        if (!proc::enter_project_root(root, err)) {
            std::cerr << "controller error: " << err << "\n";
            return 1;
        }
        Controller ctl;
        const size_t n = args.size();
        if (cmd == "coordinator") {
            uint16_t port = (n >= 3) ? static_cast<uint16_t>(std::stoi(args[2])) : 0;
            std::string bind_ip = (n >= 4) ? args[3] : "";
            return ctl.cmd_coordinator(port, bind_ip);
        }
        if (cmd == "peers") {
            std::string coord_ip = (n >= 3) ? args[2] : "";
            uint16_t port = (n >= 4) ? static_cast<uint16_t>(std::stoi(args[3])) : 0;
            return ctl.cmd_peers(coord_ip, port);
        }
        if (cmd == "chunk" && n == 3) return ctl.cmd_chunk(args[2]);
        if (cmd == "distribute") return ctl.cmd_distribute();
        if (cmd == "ping") return ctl.cmd_ping();
        if (cmd == "detect") {
            bool all = false;
            long long duration_ms = 0;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--all") all = true;
                else if (args[i] == "--duration-ms" && i + 1 < n) duration_ms = std::atoll(args[++i].c_str());
            }
            return ctl.cmd_detect(!all, duration_ms);
        }
        if (cmd == "fail" && n == 3) return ctl.cmd_fail(args[2]);
        if (cmd == "sys" && n >= 3) {
            const std::string sub = args[2];
            if (sub == "start")  return ctl.cmd_sys_start();
            if (sub == "stop")   return ctl.cmd_sys_stop();
            if (sub == "status") return ctl.cmd_sys_status();
        }
        if (cmd == "recover") {
            std::string failed;
            bool interrupt = false;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--failed" && i + 1 < n) failed = args[++i];
                else if (args[i] == "--simulate-interrupt") interrupt = true;
            }
            return ctl.cmd_recover(failed, interrupt);
        }
        if (cmd == "reconstruct")
            return ctl.cmd_reconstruct(n > 2 ? args[2] : "");
        if (cmd == "demo" && n <= 3) return ctl.cmd_demo(n == 3 ? args[2] : "");
        if (cmd == "test")
            return ctl.cmd_test(std::vector<std::string>(args.begin() + 2, args.end()));
        if (cmd == "status") return ctl.cmd_status();
        if (cmd == "reset") return ctl.cmd_reset();
        usage();
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "controller error: " << e.what() << "\n";
        return 1;
    }
}
