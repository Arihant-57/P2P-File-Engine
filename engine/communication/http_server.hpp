// ============================================================================
// http_server.hpp - Lightweight Per-Peer Local HTTP/1.1 Server (Interface B)
//
// Serves the peer's local Web Dashboard and /api/v1/* REST endpoints directly
// from each peer.exe process without any external web server or coordinator.
//
// Features:
//   - Built on native Winsock2 / POSIX sockets via socket_compat.hpp
//   - Bound to 127.0.0.1 by default (opt-in LAN mode with token auth)
//   - Streaming file uploads directly to disk staging (bounded 64 KB buffer)
//   - Streaming file downloads directly from disk (bounded 64 KB buffer)
//   - Supports both raw binary uploads and multipart/form-data uploads
//   - Bounded concurrency, request size limits, and clean shutdown
// ============================================================================
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "socket_compat.hpp"

struct HttpRequest {
    std::string method;
    std::string raw_target;
    std::string path;
    std::map<std::string, std::string> query;
    std::map<std::string, std::string> headers; // lowercase keys
    std::string remote_ip;
    std::string body;

    // Populated when a streaming upload (POST /api/v1/uploads) writes directly to disk
    std::string temp_file_path;
    std::string upload_filename;
    std::string uploaded_filename; // Alias for upload_filename
    uint64_t body_bytes_written = 0;

    std::string get_header(const std::string& lower_key, const std::string& def = "") const {
        auto it = headers.find(lower_key);
        return (it != headers.end()) ? it->second : def;
    }

    std::string header(const std::string& lower_key, const std::string& def = "") const {
        return get_header(lower_key, def);
    }

    std::string get_query(const std::string& key, const std::string& def = "") const {
        auto it = query.find(key);
        return (it != query.end()) ? it->second : def;
    }
};

struct HttpResponse {
    int status_code = 200;
    int status = 200; // Alias for status_code
    std::string status_text = "OK";
    std::string content_type = "application/json; charset=utf-8";
    std::map<std::string, std::string> headers;
    std::string body;

    int effective_status() const {
        return (status != 200) ? status : status_code;
    }

    // Optional file path to stream in 64 KB chunks as the response body
    std::string stream_file_path;
    bool delete_stream_file_after = false;

    void set_json(int code, const std::string& json_body, const std::string& text = "") {
        status_code = code;
        status = code;
        status_text = text.empty() ? default_status_text(code) : text;
        content_type = "application/json; charset=utf-8";
        body = json_body;
    }

    static std::string default_status_text(int code) {
        switch (code) {
            case 200: return "OK";
            case 201: return "Created";
            case 204: return "No Content";
            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 409: return "Conflict";
            case 413: return "Payload Too Large";
            case 422: return "Unprocessable Entity";
            case 500: return "Internal Server Error";
            case 503: return "Service Unavailable";
            default:  return "OK";
        }
    }
};

class HttpServer {
public:
    using Handler = std::function<void(const HttpRequest& req, HttpResponse& resp)>;
    using UploadProgressCallback = std::function<void(uint64_t bytes_received, uint64_t total_bytes)>;

    HttpServer(std::string bind_address,
               uint16_t port,
               std::string staging_dir,
               Handler handler,
               uint64_t max_upload_bytes = 128ULL * 1024ULL * 1024ULL);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    // Bind and start background accept loop thread.
    // If allow_port_fallback is true and the initial port is busy, scans upward for a free port.
    bool start(bool allow_port_fallback = false);
    void stop();

    bool is_running() const { return running_.load(); }
    uint16_t bound_port() const { return port_; }
    const std::string& bind_address() const { return address_; }

    static std::string url_decode(const std::string& in);

private:
    std::string address_;
    uint16_t port_;
    std::string staging_dir_;
    Handler handler_;
    uint64_t max_upload_bytes_;

    socket_t listen_fd_ = INVALID_SOCK;
    std::mutex listen_mu_;
    std::atomic<bool> running_{false};
    std::atomic<int> active_{0};
    std::thread accept_thread_;

    void accept_loop();
    void serve_client(socket_t client_fd, std::string remote_ip);
    void close_listener();
};
