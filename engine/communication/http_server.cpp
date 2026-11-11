// ============================================================================
// http_server.cpp - Lightweight Per-Peer Local HTTP/1.1 Server Implementation
// ============================================================================
#include "http_server.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "protocol.hpp"

namespace fs = std::filesystem;

namespace {

std::string to_lower_ascii(std::string s) {
    for (char& c : s) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    }
    return s;
}

std::string trim_ascii(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n')) {
        ++start;
    }
    size_t end = s.size();
    while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\r' || s[end - 1] == '\n')) {
        --end;
    }
    return s.substr(start, end - start);
}

int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

void send_raw_response(socket_t fd, const HttpResponse& resp) {
    std::error_code ec;
    uint64_t content_len = resp.body.size();
    bool use_file_stream = false;
    if (!resp.stream_file_path.empty() && fs::is_regular_file(fs::u8path(resp.stream_file_path), ec)) {
        content_len = static_cast<uint64_t>(fs::file_size(fs::u8path(resp.stream_file_path), ec));
        use_file_stream = !ec;
    }

    std::ostringstream hdr;
    const int code = resp.effective_status();
    hdr << "HTTP/1.1 " << code << " "
        << (resp.status_text.empty() ? HttpResponse::default_status_text(code) : resp.status_text)
        << "\r\n";
    hdr << "Content-Type: " << resp.content_type << "\r\n";
    hdr << "Content-Length: " << content_len << "\r\n";
    hdr << "Connection: close\r\n";
    hdr << "X-Content-Type-Options: nosniff\r\n";
    hdr << "X-Frame-Options: DENY\r\n";
    hdr << "Referrer-Policy: same-origin\r\n";
    for (const auto& kv : resp.headers) {
        hdr << kv.first << ": " << kv.second << "\r\n";
    }
    hdr << "\r\n";

    const std::string hdr_str = hdr.str();
    if (!proto::send_all(fd, hdr_str.data(), hdr_str.size())) {
        if (resp.delete_stream_file_after && !resp.stream_file_path.empty()) {
            fs::remove(fs::u8path(resp.stream_file_path), ec);
        }
        return;
    }

    if (use_file_stream) {
        std::ifstream in(fs::u8path(resp.stream_file_path), std::ios::binary);
        if (in) {
            std::vector<char> buf(65536);
            while (in) {
                in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
                const std::streamsize got = in.gcount();
                if (got <= 0) break;
                if (!proto::send_all(fd, buf.data(), static_cast<size_t>(got))) break;
            }
        }
        if (resp.delete_stream_file_after) {
            in.close();
            fs::remove(fs::u8path(resp.stream_file_path), ec);
        }
    } else if (!resp.body.empty()) {
        proto::send_all(fd, resp.body.data(), resp.body.size());
    }
}

// Extract multipart/form-data payload from raw uploaded file on disk into clean_path
bool extract_multipart_file(const std::string& raw_path,
                            const std::string& boundary,
                            const std::string& clean_path,
                            std::string& filename_out,
                            uint64_t& bytes_out) {
    std::ifstream in(fs::u8path(raw_path), std::ios::binary);
    if (!in) return false;

    // Read first 8 KB to locate part headers and \r\n\r\n
    std::vector<char> head_buf(8192, 0);
    in.read(head_buf.data(), static_cast<std::streamsize>(head_buf.size()));
    const size_t head_got = static_cast<size_t>(in.gcount());
    const std::string head_str(head_buf.data(), head_got);

    const std::string delim = "--" + boundary;
    const size_t b_pos = head_str.find(delim);
    if (b_pos == std::string::npos) return false;

    const size_t hdr_end = head_str.find("\r\n\r\n", b_pos);
    if (hdr_end == std::string::npos) return false;

    const std::string part_headers = head_str.substr(b_pos, hdr_end - b_pos);
    const size_t fn_pos = part_headers.find("filename=\"");
    if (fn_pos != std::string::npos) {
        const size_t start = fn_pos + 10;
        const size_t end = part_headers.find('"', start);
        if (end != std::string::npos) {
            filename_out = part_headers.substr(start, end - start);
        }
    }

    const uint64_t data_start = static_cast<uint64_t>(hdr_end + 4);
    std::error_code ec;
    const uint64_t total_raw_size = static_cast<uint64_t>(fs::file_size(fs::u8path(raw_path), ec));
    if (total_raw_size < data_start) return false;

    // Read tail of file (last 512 bytes) to find closing boundary "\r\n--" + boundary
    const uint64_t tail_Window = std::min<uint64_t>(512, total_raw_size - data_start);
    const uint64_t tail_offset = total_raw_size - tail_Window;
    in.clear();
    in.seekg(static_cast<std::streamoff>(tail_offset), std::ios::beg);
    std::vector<char> tail_buf(static_cast<size_t>(tail_Window), 0);
    in.read(tail_buf.data(), static_cast<std::streamsize>(tail_buf.size()));
    const std::string tail_str(tail_buf.data(), static_cast<size_t>(in.gcount()));

    const std::string close_marker = "\r\n--" + boundary;
    const size_t close_pos = tail_str.rfind(close_marker);
    uint64_t data_end = total_raw_size;
    if (close_pos != std::string::npos) {
        data_end = tail_offset + static_cast<uint64_t>(close_pos);
    }
    if (data_end < data_start) return false;

    const uint64_t payload_len = data_end - data_start;
    std::ofstream out(fs::u8path(clean_path), std::ios::binary | std::ios::trunc);
    if (!out) return false;

    in.clear();
    in.seekg(static_cast<std::streamoff>(data_start), std::ios::beg);
    std::vector<char> copy_buf(65536);
    uint64_t remaining = payload_len;
    while (remaining > 0 && in) {
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(remaining, copy_buf.size()));
        in.read(copy_buf.data(), static_cast<std::streamsize>(chunk));
        const std::streamsize got = in.gcount();
        if (got <= 0) break;
        out.write(copy_buf.data(), got);
        remaining -= static_cast<uint64_t>(got);
    }
    out.flush();
    bytes_out = payload_len - remaining;
    return remaining == 0;
}

} // namespace

HttpServer::HttpServer(std::string bind_address,
                       uint16_t port,
                       std::string staging_dir,
                       Handler handler,
                       uint64_t max_upload_bytes)
    : address_(std::move(bind_address)),
      port_(port),
      staging_dir_(std::move(staging_dir)),
      handler_(std::move(handler)),
      max_upload_bytes_(max_upload_bytes) {}

HttpServer::~HttpServer() { stop(); }

std::string HttpServer::url_decode(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%' && i + 2 < in.size()) {
            const int hi = hex_val(in[i + 1]);
            const int lo = hex_val(in[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        } else if (in[i] == '+') {
            out.push_back(' ');
            continue;
        }
        out.push_back(in[i]);
    }
    return out;
}

bool HttpServer::start(bool allow_port_fallback) {
    if (running_.load()) return true;
    if (!sock_init()) return false;

    const int max_port_tries = allow_port_fallback ? 25 : 1;
    const int bind_retries = allow_port_fallback ? 2 : 15;

    for (int pt = 0; pt < max_port_tries; ++pt) {
        const uint16_t candidate_port = static_cast<uint16_t>(port_ + pt);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(candidate_port);
        if (::inet_pton(AF_INET, address_.c_str(), &addr.sin_addr) != 1) return false;

        for (int attempt = 0; attempt < bind_retries; ++attempt) {
            socket_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd == INVALID_SOCK) return false;
#ifndef _WIN32
            int opt = 1;
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                       reinterpret_cast<const char*>(&opt), sizeof(opt));
#endif
            if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
                ::listen(fd, 32) == 0) {
                std::lock_guard<std::mutex> lk(listen_mu_);
                listen_fd_ = fd;
                port_ = candidate_port;
                running_.store(true);
                accept_thread_ = std::thread(&HttpServer::accept_loop, this);
                return true;
            }
            const int e = sock_last_error();
            sock_close(fd);
            if (!sock_err_addr_in_use(e)) break;
            if (!allow_port_fallback) {
                std::this_thread::sleep_for(std::chrono::milliseconds(80));
            }
        }
    }
    return false;
}

void HttpServer::close_listener() {
    std::lock_guard<std::mutex> lk(listen_mu_);
    if (listen_fd_ != INVALID_SOCK) {
        sock_close(listen_fd_);
        listen_fd_ = INVALID_SOCK;
    }
}

void HttpServer::stop() {
    const bool was_running = running_.exchange(false);
    if (was_running) {
        socket_t w = ::socket(AF_INET, SOCK_STREAM, 0);
        if (w != INVALID_SOCK) {
            sockaddr_in a{};
            a.sin_family = AF_INET;
            a.sin_port = htons(port_);
            const char* host = (address_ == "0.0.0.0") ? "127.0.0.1" : address_.c_str();
            ::inet_pton(AF_INET, host, &a.sin_addr);
            ::connect(w, reinterpret_cast<sockaddr*>(&a), sizeof(a));
            sock_close(w);
        }
    }
    close_listener();
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    for (int i = 0; i < 100 && active_.load() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void HttpServer::accept_loop() {
    socket_t lfd = INVALID_SOCK;
    {
        std::lock_guard<std::mutex> lk(listen_mu_);
        lfd = listen_fd_;
    }
    int consecutive_errors = 0;
    while (running_.load() && lfd != INVALID_SOCK) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        socket_t client = ::accept(lfd, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client == INVALID_SOCK) {
            if (!running_.load()) break;
            if (++consecutive_errors > 50) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        consecutive_errors = 0;
        if (!running_.load()) {
            sock_close(client);
            break;
        }

        char ip_buf[64] = {0};
        ::inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));
        std::string remote_ip = ip_buf[0] ? std::string(ip_buf) : "127.0.0.1";

        if (active_.load() >= 32) {
            HttpResponse busy_resp;
            busy_resp.set_json(503, "{\"status\":\"FAILED\",\"reason\":\"too many concurrent HTTP connections\"}");
            send_raw_response(client, busy_resp);
            sock_close(client);
            continue;
        }

        ++active_;
        try {
            std::thread([this, client, remote_ip]() {
                serve_client(client, remote_ip);
                --active_;
            }).detach();
        } catch (...) {
            --active_;
            sock_close(client);
        }
    }
    close_listener();
}

void HttpServer::serve_client(socket_t client_fd, std::string remote_ip) {
    proto::tune_socket(client_fd);
    proto::set_timeouts(client_fd, 30000, 120000);

    // 1. Read HTTP header block up to "\r\n\r\n" (bounded to 32 KB)
    std::string raw_head;
    raw_head.reserve(4096);
    char buf[4096];
    size_t header_end_pos = std::string::npos;

    while (raw_head.size() < 32768) {
        const int n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            sock_close(client_fd);
            return;
        }
        raw_head.append(buf, static_cast<size_t>(n));
        header_end_pos = raw_head.find("\r\n\r\n");
        if (header_end_pos != std::string::npos) break;
    }

    if (header_end_pos == std::string::npos) {
        HttpResponse bad;
        bad.set_json(400, "{\"status\":\"FAILED\",\"reason\":\"HTTP request header too large or malformed\"}");
        send_raw_response(client_fd, bad);
        sock_close(client_fd);
        return;
    }

    const std::string header_block = raw_head.substr(0, header_end_pos);
    const std::string initial_body = raw_head.substr(header_end_pos + 4);

    // 2. Parse request line and headers
    HttpRequest req;
    req.remote_ip = std::move(remote_ip);

    std::istringstream hstream(header_block);
    std::string req_line;
    if (!std::getline(hstream, req_line)) {
        sock_close(client_fd);
        return;
    }
    if (!req_line.empty() && req_line.back() == '\r') req_line.pop_back();

    {
        std::istringstream rl(req_line);
        std::string http_ver;
        if (!(rl >> req.method >> req.raw_target >> http_ver)) {
            HttpResponse bad;
            bad.set_json(400, "{\"status\":\"FAILED\",\"reason\":\"malformed HTTP request line\"}");
            send_raw_response(client_fd, bad);
            sock_close(client_fd);
            return;
        }
    }

    // Split raw_target into path and query parameters
    const size_t qmark = req.raw_target.find('?');
    if (qmark == std::string::npos) {
        req.path = url_decode(req.raw_target);
    } else {
        req.path = url_decode(req.raw_target.substr(0, qmark));
        const std::string qstr = req.raw_target.substr(qmark + 1);
        size_t pos = 0;
        while (pos < qstr.size()) {
            const size_t amp = qstr.find('&', pos);
            const std::string pair = (amp == std::string::npos) ? qstr.substr(pos) : qstr.substr(pos, amp - pos);
            if (!pair.empty()) {
                const size_t eq = pair.find('=');
                if (eq == std::string::npos) {
                    req.query[url_decode(pair)] = "";
                } else {
                    req.query[url_decode(pair.substr(0, eq))] = url_decode(pair.substr(eq + 1));
                }
            }
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
    }

    std::string line;
    while (std::getline(hstream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;
        const size_t colon = line.find(':');
        if (colon != std::string::npos) {
            const std::string k = to_lower_ascii(trim_ascii(line.substr(0, colon)));
            const std::string v = trim_ascii(line.substr(colon + 1));
            req.headers[k] = v;
        }
    }

    uint64_t content_length = 0;
    const std::string cl_str = req.get_header("content-length");
    if (!cl_str.empty()) {
        try {
            content_length = static_cast<uint64_t>(std::stoull(cl_str));
        } catch (...) {
            HttpResponse bad;
            bad.set_json(400, "{\"status\":\"FAILED\",\"reason\":\"invalid Content-Length header\"}");
            send_raw_response(client_fd, bad);
            sock_close(client_fd);
            return;
        }
    }

    if (content_length > max_upload_bytes_) {
        HttpResponse too_large;
        too_large.set_json(413, "{\"status\":\"FAILED\",\"reason\":\"payload exceeds maximum upload limit\"}");
        send_raw_response(client_fd, too_large);
        sock_close(client_fd);
        return;
    }

    // 3. Read request body (stream directly to disk for /api/v1/uploads; buffer small JSON for others)
    const bool is_upload_route = (req.method == "POST" && req.path == "/api/v1/uploads");
    std::error_code ec;

    if (is_upload_route) {
        fs::create_directories(fs::u8path(staging_dir_), ec);
        static std::atomic<uint64_t> s_up_seq{0};
        const uint64_t seq = ++s_up_seq;
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const std::string raw_tmp = staging_dir_ + "/http_upload_" + std::to_string(now_ms) + "_" + std::to_string(seq) + ".tmp";

        std::ofstream out(fs::u8path(raw_tmp), std::ios::binary | std::ios::trunc);
        if (!out) {
            HttpResponse err_resp;
            err_resp.set_json(500, "{\"status\":\"FAILED\",\"reason\":\"cannot create upload staging file\"}");
            send_raw_response(client_fd, err_resp);
            sock_close(client_fd);
            return;
        }

        uint64_t written = 0;
        if (!initial_body.empty()) {
            const size_t to_write = static_cast<size_t>(std::min<uint64_t>(initial_body.size(), content_length));
            out.write(initial_body.data(), static_cast<std::streamsize>(to_write));
            written += to_write;
        }

        std::vector<char> stream_buf(65536);
        while (written < content_length) {
            const size_t want = static_cast<size_t>(std::min<uint64_t>(content_length - written, stream_buf.size()));
            const int got = ::recv(client_fd, stream_buf.data(), static_cast<int>(want), 0);
            if (got <= 0) {
                out.close();
                fs::remove(fs::u8path(raw_tmp), ec);
                sock_close(client_fd);
                return;
            }
            out.write(stream_buf.data(), got);
            written += static_cast<uint64_t>(got);
        }
        out.flush();
        out.close();

        // Check if Content-Type is multipart/form-data
        const std::string ctype = req.get_header("content-type");
        const std::string ctype_low = to_lower_ascii(ctype);
        if (ctype_low.find("multipart/form-data") != std::string::npos) {
            std::string boundary;
            const size_t bpos = ctype.find("boundary=");
            if (bpos != std::string::npos) {
                boundary = trim_ascii(ctype.substr(bpos + 9));
                if (boundary.size() >= 2 && boundary.front() == '"' && boundary.back() == '"') {
                    boundary = boundary.substr(1, boundary.size() - 2);
                }
            }
            const std::string clean_tmp = raw_tmp + ".clean";
            std::string mp_filename;
            uint64_t mp_bytes = 0;
            if (!boundary.empty() && extract_multipart_file(raw_tmp, boundary, clean_tmp, mp_filename, mp_bytes)) {
                fs::remove(fs::u8path(raw_tmp), ec);
                req.temp_file_path = clean_tmp;
                req.upload_filename = mp_filename;
                req.uploaded_filename = mp_filename;
                req.body_bytes_written = mp_bytes;
            } else {
                fs::remove(fs::u8path(raw_tmp), ec);
                fs::remove(fs::u8path(clean_tmp), ec);
                HttpResponse bad_mp;
                bad_mp.set_json(400, "{\"status\":\"FAILED\",\"reason\":\"malformed multipart/form-data upload payload\"}");
                send_raw_response(client_fd, bad_mp);
                sock_close(client_fd);
                return;
            }
        } else {
            req.temp_file_path = raw_tmp;
            req.body_bytes_written = written;
            req.upload_filename = req.get_query("filename");
            if (req.upload_filename.empty()) {
                req.upload_filename = url_decode(req.get_header("x-file-name"));
            }
            req.uploaded_filename = req.upload_filename;
        }
    } else if (content_length > 0) {
        if (content_length > 1024 * 1024) {
            HttpResponse too_big;
            too_big.set_json(413, "{\"status\":\"FAILED\",\"reason\":\"JSON request body too large\"}");
            send_raw_response(client_fd, too_big);
            sock_close(client_fd);
            return;
        }
        req.body = initial_body;
        if (req.body.size() > content_length) {
            req.body.resize(static_cast<size_t>(content_length));
        }
        while (req.body.size() < content_length) {
            const size_t rem = static_cast<size_t>(content_length - req.body.size());
            const int got = ::recv(client_fd, buf, static_cast<int>(std::min(rem, sizeof(buf))), 0);
            if (got <= 0) {
                sock_close(client_fd);
                return;
            }
            req.body.append(buf, static_cast<size_t>(got));
        }
    }

    // 4. Invoke route handler and send HTTP response
    HttpResponse resp;
    try {
        handler_(req, resp);
    } catch (const std::exception& e) {
        resp.set_json(500, std::string("{\"status\":\"FAILED\",\"reason\":\"internal error: ") + e.what() + "\"}");
    }

    if (!req.temp_file_path.empty() && fs::exists(fs::u8path(req.temp_file_path), ec)) {
        fs::remove(fs::u8path(req.temp_file_path), ec);
    }

    send_raw_response(client_fd, resp);
    sock_close(client_fd);
}
