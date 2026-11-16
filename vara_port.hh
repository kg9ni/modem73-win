#pragma once

// Windows/Winsock port of the VARA-compatible host ports (from kg9ni/modem73).
// Keep in sync with the Linux vara_port.hh; regenerate via packaging/windows/mk-winsock-vara-port.py
// VARA-compatible command/data TCP host ports for linbpq64's VARA driver.
// Semantics follow kg9ni/xara (VARA HF host API): CR-terminated commands on
 // port N, raw bytes on port N+1, one client per port, RETRIES for connect
// attempts. Over-the-air sessions use a modem73-native framing (not VARA PHY).

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef SHUT_RD
#define SHUT_RD SD_RECEIVE
#endif
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
#ifndef socklen_t
using socklen_t = int;
#endif
#ifndef _SSIZE_T_DEFINED
using ssize_t = int;
#endif


// Winsock helpers (modem73-win uses int FDs + closesocket like control_port.hh).
static inline void sock_close(int fd) {
    if (fd >= 0) closesocket(fd);
}
static inline std::string sock_err() {
    return "WSA " + std::to_string(WSAGetLastError());
}

class VaraPort {
public:
    static constexpr int MAX_LINE = 1024;
    static constexpr size_t DATA_CHUNK = 65536;
    static constexpr uint32_t DEFAULT_RETRIES = 15;
    static constexpr int CONNECT_RETRY_MS = 8000;
    static constexpr int REGISTERED_AFTER_MS = 4000;
    static constexpr int IAMALIVE_MS = 60000;
    static constexpr size_t MAX_HOST_TX = 1u << 20;
    static constexpr const char* VERSION_HF = "VERSION VARA HF v4.9.0";

    struct Hooks {
        std::function<void(const std::vector<uint8_t>&)> queue_tx;
        std::function<bool()> is_ptt;
        std::function<bool()> is_busy;
        std::function<size_t()> payload_capacity;
        std::function<void(float)> set_drive; // linear 0.05..1.0
        std::function<float()> get_drive;
    };

    VaraPort(int cmd_port, const std::string& bind_address, uint32_t retries, Hooks hooks)
        : cmd_port_(cmd_port), bind_address_(bind_address),
          retries_(retries && retries <= 255 ? retries : DEFAULT_RETRIES),
          hooks_(std::move(hooks)), saved_retries_(retries_) {}

    ~VaraPort() { stop(); }

    bool start() {
        if (cmd_port_ <= 0 || cmd_port_ >= 65535) {
            std::cerr << "vara: command port must be 1..65534 (data uses port+1)" << std::endl;
            return false;
        }
        cmd_fd_ = bind_listen(cmd_port_);
        if (cmd_fd_ < 0) return false;
        data_fd_ = bind_listen(cmd_port_ + 1);
        if (data_fd_ < 0) {
            sock_close(cmd_fd_);
            cmd_fd_ = -1;
            return false;
        }
        running_ = true;
        accept_cmd_ = std::thread(&VaraPort::accept_loop, this, true);
        accept_data_ = std::thread(&VaraPort::accept_loop, this, false);
        service_ = std::thread(&VaraPort::service_loop, this);
        std::cerr << "VARA command port on " << bind_address_ << ":" << cmd_port_
                  << " (data " << (cmd_port_ + 1) << ", retries " << retries_ << ")" << std::endl;
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        wake(cmd_port_);
        wake(cmd_port_ + 1);
        {
            std::lock_guard<std::mutex> lock(mu_);
            close_clients_locked();
            cv_.notify_all();
        }
        if (accept_cmd_.joinable()) accept_cmd_.join();
        if (accept_data_.joinable()) accept_data_.join();
        if (service_.joinable()) service_.join();
        if (cmd_reader_.joinable()) cmd_reader_.join();
        if (data_reader_.joinable()) data_reader_.join();
        if (cmd_fd_ >= 0) { sock_close(cmd_fd_); cmd_fd_ = -1; }
        if (data_fd_ >= 0) { sock_close(data_fd_); data_fd_ = -1; }
    }

    // Consume modem73 RX payloads that belong to the VARA session bridge.
    // Returns true when the frame must not be forwarded to KISS clients.
    bool handle_rx(const std::vector<uint8_t>& payload) {
        if (payload.size() < 4) return false;
        if (payload[0] != 'V' || payload[1] != '7' || payload[2] != '3') return false;
        uint8_t type = payload[3];
        std::lock_guard<std::mutex> lock(mu_);
        switch (type) {
            case 1: // CONN_REQ
                on_conn_req_locked(payload);
                return true;
            case 2: // CONN_ACK
                on_conn_ack_locked(payload);
                return true;
            case 3: // DATA
                on_data_locked(payload);
                return true;
            case 4: // DISC
            case 5: // ABORT
                if (state_ == State::Connected || state_ == State::Calling || state_ == State::Answering) {
                    finish_disconnect_locked(type == 5);
                }
                return true;
            default:
                return true;
        }
    }

    uint32_t retries() const {
        std::lock_guard<std::mutex> lock(mu_);
        return retries_;
    }

private:
    enum class State { Idle, Listen, Calling, Answering, Connected };

    enum class Comp { Off, Text, Files };

    int cmd_port_ = 0;
    std::string bind_address_ = "127.0.0.1";
    uint32_t retries_ = DEFAULT_RETRIES;
    Hooks hooks_;

    std::atomic<bool> running_{false};
    int cmd_fd_ = -1;
    int data_fd_ = -1;
    std::thread accept_cmd_;
    std::thread accept_data_;
    std::thread service_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    int cmd_client_ = -1;
    int data_client_ = -1;
    std::thread cmd_reader_;
    std::thread data_reader_;
    bool cmd_paused_ = false;
    bool data_paused_ = false;

    State state_ = State::Idle;
    bool listen_ = false;
    bool chat_ = false;
    bool p2p_ = false;
    bool busy_announced_ = false;
    bool last_ptt_ = false;
    bool last_busy_ = false;
    uint32_t bw_hz_ = 2300;
    uint32_t saved_retries_ = DEFAULT_RETRIES;
    uint32_t connect_attempts_ = 0;
    Comp compression_ = Comp::Text;
    std::vector<std::string> mycalls_;
    std::string peer_call_;
    std::string local_call_;
    std::deque<uint8_t> host_tx_;
    std::vector<uint8_t> cmd_line_;
    bool cmd_line_long_ = false;

    std::chrono::steady_clock::time_point next_retry_{};
    std::chrono::steady_clock::time_point registered_at_{};
    bool registered_pending_ = false;
    std::chrono::steady_clock::time_point next_alive_{};
    bool have_clients_ = false;

    int bind_listen(int port) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            std::cerr << "vara: socket failed: " << sock_err() << std::endl;
            return -1;
        }
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        if (inet_pton(AF_INET, bind_address_.c_str(), &addr.sin_addr) != 1) {
            std::cerr << "vara: invalid bind address " << bind_address_ << std::endl;
            sock_close(fd);
            return -1;
        }
        addr.sin_port = htons((uint16_t)port);
        if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "vara: bind " << bind_address_ << ":" << port << " failed: "
                      << sock_err() << std::endl;
            sock_close(fd);
            return -1;
        }
        if (listen(fd, 128) < 0) {
            std::cerr << "vara: listen failed: " << sock_err() << std::endl;
            sock_close(fd);
            return -1;
        }
        return fd;
    }

    void wake(int port) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        addr.sin_port = htons((uint16_t)port);
        int s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) return;
        DWORD tv_ms = 100;
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv_ms, sizeof(tv_ms));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv_ms, sizeof(tv_ms));
        connect(s, (sockaddr*)&addr, sizeof(addr));
        sock_close(s);
    }

    void pause_accept(int listen_fd, bool& paused) {
        if (paused || listen_fd < 0) return;
        int off = 0;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&off, sizeof(off));
        shutdown(listen_fd, SHUT_RD);
        paused = true;
    }

    void resume_accept(int listen_fd, bool& paused) {
        if (!paused || listen_fd < 0) return;
        int on = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&on, sizeof(on));
        listen(listen_fd, 128);
        paused = false;
    }

    void accept_loop(bool is_cmd) {
        int& lfd = is_cmd ? cmd_fd_ : data_fd_;
        while (running_) {
            sockaddr_in ca{};
            socklen_t cl = sizeof(ca);
            int cfd = accept(lfd, (sockaddr*)&ca, &cl);
            if (!running_) {
                if (cfd >= 0) sock_close(cfd);
                return;
            }
            if (cfd < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            int flag = 1;
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, (const char*)&flag, sizeof(flag));

            std::unique_lock<std::mutex> lock(mu_);
            int& slot = is_cmd ? cmd_client_ : data_client_;
            if (slot >= 0) {
                // Should be rare (listen paused); drop immediately.
                lock.unlock();
                sock_close(cfd);
                continue;
            }
            slot = cfd;
            pause_accept(lfd, is_cmd ? cmd_paused_ : data_paused_);
            std::thread old;
            if (is_cmd) {
                old = std::move(cmd_reader_);
                cmd_reader_ = std::thread(&VaraPort::read_commands, this, cfd);
            } else {
                old = std::move(data_reader_);
                data_reader_ = std::thread(&VaraPort::read_data, this, cfd);
            }
            on_client_change_locked();
            lock.unlock();
            if (old.joinable()) old.join();
            // Wait until this client slot clears, then resume listen.
            while (running_) {
                {
                    std::lock_guard<std::mutex> g(mu_);
                    if ((is_cmd ? cmd_client_ : data_client_) < 0) break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            {
                std::lock_guard<std::mutex> g(mu_);
                resume_accept(lfd, is_cmd ? cmd_paused_ : data_paused_);
            }
        }
    }

    void on_client_change_locked() {
        bool both = cmd_client_ >= 0 && data_client_ >= 0;
        bool any = cmd_client_ >= 0 || data_client_ >= 0;
        if (both && !have_clients_) {
            have_clients_ = true;
            next_alive_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(IAMALIVE_MS);
            // Match xara: connecting a host toggles BUSY once.
            busy_announced_ = !busy_announced_;
            say_locked(busy_announced_ ? "BUSY ON" : "BUSY OFF");
        }
        if (!any && have_clients_) {
            have_clients_ = false;
            host_dropped_locked();
        }
        // Dropping either side closes the pair (xara behavior).
        if (have_clients_ && (cmd_client_ < 0 || data_client_ < 0)) {
            close_clients_locked();
            have_clients_ = false;
            host_dropped_locked();
        }
        cv_.notify_all();
    }

    void host_dropped_locked() {
        listen_ = false;
        chat_ = false;
        if (state_ != State::Idle) {
            state_ = State::Idle;
            peer_call_.clear();
            local_call_.clear();
            host_tx_.clear();
            connect_attempts_ = 0;
        }
    }

    void close_clients_locked() {
        auto kill = [](int& fd) {
            if (fd >= 0) {
                shutdown(fd, SHUT_RDWR);
                sock_close(fd);
                fd = -1;
            }
        };
        kill(cmd_client_);
        kill(data_client_);
    }

    void read_commands(int fd) {
        uint8_t buf[4096];
        while (running_) {
            ssize_t n = recv(fd, (char*)buf, sizeof(buf), 0);
            if (n <= 0) break;
            std::lock_guard<std::mutex> lock(mu_);
            for (ssize_t i = 0; i < n; ++i) {
                uint8_t b = buf[i];
                if (b == '\r') {
                    std::string line(cmd_line_.begin(), cmd_line_.end());
                    cmd_line_.clear();
                    bool long_line = cmd_line_long_;
                    cmd_line_long_ = false;
                    if (!long_line) handle_line_locked(line);
                } else if (b == '\n') {
                    // ignored (CR framing)
                } else if (cmd_line_.size() >= (size_t)MAX_LINE) {
                    cmd_line_.clear();
                    cmd_line_long_ = true;
                } else if (!cmd_line_long_) {
                    cmd_line_.push_back(b);
                }
            }
        }
        std::lock_guard<std::mutex> lock(mu_);
        if (cmd_client_ == fd) {
            cmd_client_ = -1;
            on_client_change_locked();
        }
    }

    void read_data(int fd) {
        std::vector<uint8_t> buf(DATA_CHUNK);
        while (running_) {
            ssize_t n = recv(fd, (char*)buf.data(), buf.size(), 0);
            if (n <= 0) break;
            std::lock_guard<std::mutex> lock(mu_);
            if (host_tx_.size() + (size_t)n > MAX_HOST_TX) {
                // Drop overflow; keep BUFFER honest for what we hold.
                n = (ssize_t)std::min((size_t)n, MAX_HOST_TX - host_tx_.size());
            }
            host_tx_.insert(host_tx_.end(), buf.begin(), buf.begin() + n);
            report_buffer_locked();
            pump_tx_locked();
        }
        std::lock_guard<std::mutex> lock(mu_);
        if (data_client_ == fd) {
            data_client_ = -1;
            on_client_change_locked();
        }
    }

    void say_locked(const std::string& s) {
        if (cmd_client_ < 0) return;
        std::string out = s;
        out.push_back('\r');
        ssize_t off = 0;
        while (off < (ssize_t)out.size()) {
            ssize_t n = send(cmd_client_, out.data() + off, out.size() - off, MSG_NOSIGNAL);
            if (n <= 0) break;
            off += n;
        }
    }

    void data_out_locked(const uint8_t* p, size_t n) {
        if (data_client_ < 0 || n == 0) return;
        size_t off = 0;
        while (off < n) {
            ssize_t w = send(data_client_, (const char*)(p + off), n - off, MSG_NOSIGNAL);
            if (w <= 0) break;
            off += (size_t)w;
        }
    }

    void report_buffer_locked() {
        say_locked("BUFFER " + std::to_string(host_tx_.size()));
    }

    static std::string fix_suffix(const std::string& t) {
        if (t.size() == 8 && t.find('-') == std::string::npos) {
            char c = t[7];
            if (c == 'R' || c == 'T' || c == 'X' || c == 'L' ||
                c == 'r' || c == 't' || c == 'x' || c == 'l')
                return t.substr(0, 7) + "-" + t.substr(7);
        }
        return t;
    }

    static std::optional<std::string> valid_call(std::string t) {
        for (char& c : t) c = (char)toupper((unsigned char)c);
        t = fix_suffix(t);
        size_t dash = t.find('-');
        std::string base = dash == std::string::npos ? t : t.substr(0, dash);
        std::string suf = dash == std::string::npos ? "" : t.substr(dash + 1);
        if (base.size() < 1 || base.size() > 7) return std::nullopt;
        for (unsigned char c : base)
            if (!(std::isupper(c) || std::isdigit(c))) return std::nullopt;
        if (suf.empty()) return t;
        if (suf.size() == 1) {
            char c = suf[0];
            if (c == 'T' || c == 'R' || c == 'X' || c == 'L') return t;
            if (c >= '1' && c <= '9') return t;
            return std::nullopt;
        }
        if (suf.size() == 2 && std::isdigit((unsigned char)suf[0]) && std::isdigit((unsigned char)suf[1])) {
            int n = (suf[0] - '0') * 10 + (suf[1] - '0');
            if (n >= 1 && n <= 19) return t;
        }
        return std::nullopt;
    }

    static std::optional<double> vb_num(std::string s) {
        std::string cleaned;
        for (char c : s)
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') cleaned.push_back(c);
        if (cleaned.empty()) return std::nullopt;
        char* end = nullptr;
        double v = strtod(cleaned.c_str(), &end);
        if (end == cleaned.c_str()) return std::nullopt;
        return v;
    }

    bool idle_locked() const {
        return state_ == State::Idle || state_ == State::Listen;
    }

    void handle_line_locked(const std::string& line) {
        if (line == "VERSION") {
            say_locked(VERSION_HF);
            return;
        }
        if (line == "DISCONNECT") {
            if (state_ == State::Calling) {
                connect_attempts_ = retries_; // abort calling like xara
            }
            if (state_ == State::Connected || state_ == State::Answering) {
                send_ctrl_locked(4);
            }
            finish_disconnect_locked(false);
            say_locked("OK");
            return;
        }
        if (line == "ABORT") {
            if (state_ == State::Calling) connect_attempts_ = retries_;
            if (state_ == State::Connected || state_ == State::Answering || state_ == State::Calling)
                send_ctrl_locked(5);
            host_tx_.clear();
            finish_disconnect_locked(true);
            say_locked("OK");
            return;
        }
        if (line == "LISTEN ON") {
            bool linked = !idle_locked();
            listen_ = true;
            if (state_ == State::Idle) state_ = State::Listen;
            say_locked("OK");
            if (linked) finish_disconnect_locked(true);
            return;
        }
        if (line == "LISTEN OFF") {
            bool linked = !idle_locked();
            listen_ = false;
            if (state_ == State::Listen) state_ = State::Idle;
            say_locked("OK");
            if (linked) finish_disconnect_locked(true);
            return;
        }
        if (line == "LISTEN CQ" || line == "CHAT ON") {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            chat_ = true;
            listen_ = true;
            state_ = State::Listen;
            busy_announced_ = !busy_announced_;
            say_locked("OK");
            say_locked(busy_announced_ ? "BUSY ON" : "BUSY OFF");
            return;
        }
        if (line == "CHAT OFF") {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            chat_ = false;
            say_locked("OK");
            return;
        }
        if (line == "WINLINK SESSION") {
            p2p_ = false;
            chat_ = true;
            say_locked("OK");
            return;
        }
        if (line == "P2P SESSION") {
            p2p_ = true;
            say_locked("OK");
            return;
        }
        if (line == "PUBLIC ON" || line == "PUBLIC OFF" ||
            line == "CWID ON" || line == "CWID OFF" ||
            line.rfind("GATEWAY ", 0) == 0) {
            say_locked("OK");
            return;
        }
        if (line == "COMPRESSION OFF") { compression_ = Comp::Off; say_locked("OK"); return; }
        if (line == "COMPRESSION ON" || line == "COMPRESSION TEXT") {
            compression_ = Comp::Text; say_locked("OK"); return;
        }
        if (line == "COMPRESSION FILES") { compression_ = Comp::Files; say_locked("OK"); return; }
        if (line == "IGNOREKISSDCD ON" || line == "IGNOREKISSDCD OFF") {
            say_locked("OK");
            return;
        }
        if (line == "BW500") {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            bw_hz_ = 500; say_locked("OK"); return;
        }
        if (line == "BW2300") {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            bw_hz_ = 2300; say_locked("OK"); return;
        }
        if (line == "BW2750") {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            bw_hz_ = 2750; say_locked("OK"); return;
        }
        if (line == "CLEANTXBUFFER") {
            if (host_tx_.empty()) say_locked("CLEANTXBUFFER BUFFEREMPTY");
            else say_locked("CLEANTXBUFFER FAILED ");
            return;
        }
        if (line == "TUNE OFF") { say_locked("OK"); return; }
        if (line == "TUNE ?") {
            float d = hooks_.get_drive ? hooks_.get_drive() : 1.0f;
            double db = 20.0 * std::log10(std::max(0.05f, d));
            char b[32];
            snprintf(b, sizeof(b), "TUNE %.0f", std::round(db));
            say_locked(b);
            return;
        }
        if (line.rfind("TUNE ", 0) == 0) {
            say_locked("OK");
            apply_drive_locked(line.substr(5));
            return;
        }
        if (line.rfind("DRIVELEVEL ", 0) == 0) {
            apply_drive_locked(line.substr(11));
            say_locked("OK");
            return;
        }
        if (line.rfind("RETRIES ", 0) == 0) {
            apply_retries_locked(line.substr(8));
            say_locked("OK");
            return;
        }
        if (line.rfind("MYCALL ", 0) == 0) {
            handle_mycall_locked(line.substr(7));
            return;
        }
        if (line.rfind("CONNECT ", 0) == 0) {
            handle_connect_locked(line);
            return;
        }
        if (line.rfind("CQFRAME ", 0) == 0) {
            if (!idle_locked()) { say_locked("WRONG"); return; }
            say_locked("OK");
            return;
        }
        say_locked("WRONG");
    }

    void apply_retries_locked(const std::string& rest) {
        // xara/linbpq: RETRIES n (1..255) sets connect attempts for the next
        // CONNECT. RETRIES 0 restores the previous value. Non-numeric / out of
        // range leave the current value unchanged; the reply is still OK.
        auto v = vb_num(rest);
        if (!v) return;
        double r = std::round(*v);
        if (r == 0.0) {
            if (saved_retries_ > 0) retries_ = saved_retries_;
            return;
        }
        if (r < 0.0 || r > 255.0) return;
        saved_retries_ = retries_;
        retries_ = (uint32_t)r;
    }

    void apply_drive_locked(const std::string& rest) {
        std::string t = rest;
        while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) t.pop_back();
        if (t == "?" || t == "OFF" || t == "off") return;
        auto v = vb_num(t);
        if (!v || *v < -30.0 || *v > 0.0) return;
        double db = std::round(*v);
        float lin = (float)std::pow(10.0, db / 20.0);
        if (lin < 0.05f) lin = 0.05f;
        if (lin > 1.0f) lin = 1.0f;
        if (hooks_.set_drive) hooks_.set_drive(lin);
    }

    void handle_mycall_locked(const std::string& rest) {
        if (!idle_locked()) {
            say_locked("WRONG");
            return;
        }
        std::vector<std::string> parts;
        {
            std::string cur;
            for (char c : rest) {
                if (c == ' ') {
                    if (!cur.empty()) { parts.push_back(cur); cur.clear(); }
                } else cur.push_back(c);
            }
            if (!cur.empty()) parts.push_back(cur);
        }
        if (parts.empty() || parts.size() > 5) {
            say_locked("WRONG");
            say_locked("OK");
            return;
        }
        std::vector<std::string> calls;
        for (auto& p : parts) {
            auto c = valid_call(p);
            if (!c) {
                say_locked("WRONG");
                say_locked("OK");
                return;
            }
            calls.push_back(*c);
        }
        mycalls_ = std::move(calls);
        registered_pending_ = true;
        registered_at_ = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(REGISTERED_AFTER_MS);
        say_locked("OK");
    }

    void handle_connect_locked(const std::string& line) {
        // CONNECT mycall remote [VIA ...]
        std::vector<std::string> t;
        {
            std::string cur;
            for (char c : line) {
                if (c == ' ') {
                    if (!cur.empty()) { t.push_back(cur); cur.clear(); }
                } else cur.push_back(c);
            }
            if (!cur.empty()) t.push_back(cur);
        }
        if (t.size() < 3) { say_locked("WRONG"); return; }
        auto my = valid_call(t[1]);
        auto rem = valid_call(t[2]);
        if (!my || !rem) { say_locked("WRONG"); return; }
        if (!idle_locked() || (hooks_.is_ptt && hooks_.is_ptt())) {
            say_locked("WRONG");
            return;
        }
        local_call_ = *my;
        peer_call_ = *rem;
        state_ = State::Calling;
        connect_attempts_ = 0;
        say_locked("OK");
        // First attempt immediately; further attempts up to RETRIES.
        attempt_connect_locked();
    }

    void attempt_connect_locked() {
        if (state_ != State::Calling) return;
        if (connect_attempts_ >= retries_) {
            // Exhausted user/host RETRIES — give up like xara/VARA.
            finish_disconnect_locked(false);
            return;
        }
        connect_attempts_++;
        send_conn_locked(1, local_call_, peer_call_);
        next_retry_ = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(CONNECT_RETRY_MS);
    }

    bool is_mycall_locked(const std::string& c) const {
        for (auto& m : mycalls_)
            if (m == c) return true;
        return false;
    }

    void send_conn_locked(uint8_t type, const std::string& a, const std::string& b) {
        std::vector<uint8_t> f;
        f.push_back('V'); f.push_back('7'); f.push_back('3'); f.push_back(type);
        f.push_back((uint8_t)((bw_hz_ >> 8) & 0xff));
        f.push_back((uint8_t)(bw_hz_ & 0xff));
        f.insert(f.end(), a.begin(), a.end());
        f.push_back(0);
        f.insert(f.end(), b.begin(), b.end());
        f.push_back(0);
        if (hooks_.queue_tx) hooks_.queue_tx(f);
    }

    void send_ctrl_locked(uint8_t type) {
        std::vector<uint8_t> f = {'V', '7', '3', type};
        if (hooks_.queue_tx) hooks_.queue_tx(f);
    }

    void on_conn_req_locked(const std::vector<uint8_t>& p) {
        std::string caller, answerer;
        uint32_t bw = 2300;
        if (!parse_conn_locked(p, caller, answerer, bw)) return;
        if (!(listen_ || chat_)) return;
        if (!is_mycall_locked(answerer)) return;
        if (!idle_locked() && state_ != State::Listen) return;
        local_call_ = answerer;
        peer_call_ = caller;
        bw_hz_ = bw;
        state_ = State::Connected;
        send_conn_locked(2, answerer, caller);
        char line[128];
        snprintf(line, sizeof(line), "CONNECTED %s %s %u",
                 shown_call(caller).c_str(), shown_call(answerer).c_str(), bw_hz_);
        say_locked(line);
        say_locked("LINK REGISTERED");
    }

    void on_conn_ack_locked(const std::vector<uint8_t>& p) {
        if (state_ != State::Calling) return;
        std::string a, b;
        uint32_t bw = 2300;
        if (!parse_conn_locked(p, a, b, bw)) return;
        // ACK carries (answerer, caller). Accept either order for robustness.
        if (!((a == peer_call_ && b == local_call_) ||
              (a == local_call_ && b == peer_call_)))
            return;
        bw_hz_ = bw ? bw : bw_hz_;
        state_ = State::Connected;
        char line[128];
        snprintf(line, sizeof(line), "CONNECTED %s %s %u",
                 shown_call(local_call_).c_str(), shown_call(peer_call_).c_str(), bw_hz_);
        say_locked(line);
        pump_tx_locked();
    }

    void on_data_locked(const std::vector<uint8_t>& p) {
        if (state_ != State::Connected) return;
        if (p.size() < 6) return;
        uint16_t len = ((uint16_t)p[4] << 8) | p[5];
        if (p.size() < 6u + len) return;
        data_out_locked(p.data() + 6, len);
    }

    bool parse_conn_locked(const std::vector<uint8_t>& p, std::string& a, std::string& b, uint32_t& bw) {
        if (p.size() < 8) return false;
        bw = ((uint32_t)p[4] << 8) | p[5];
        size_t i = 6;
        while (i < p.size() && p[i]) a.push_back((char)p[i++]);
        if (i >= p.size() || p[i] != 0) return false;
        ++i;
        while (i < p.size() && p[i]) b.push_back((char)p[i++]);
        return !a.empty() && !b.empty();
    }

    static std::string shown_call(const std::string& c) {
        // VARA display form collapses single-letter -R/-T/-X/-L suffixes.
        if (c.size() == 9 && c[7] == '-' &&
            (c[8] == 'R' || c[8] == 'T' || c[8] == 'X' || c[8] == 'L'))
            return c.substr(0, 7) + c.substr(8);
        return c;
    }

    void finish_disconnect_locked(bool /*abort*/) {
        bool was = state_ == State::Connected || state_ == State::Calling ||
                   state_ == State::Answering;
        state_ = listen_ ? State::Listen : State::Idle;
        connect_attempts_ = 0;
        peer_call_.clear();
        local_call_.clear();
        if (was) say_locked("DISCONNECTED");
    }

    void pump_tx_locked() {
        if (state_ != State::Connected || host_tx_.empty()) return;
        size_t cap = hooks_.payload_capacity ? hooks_.payload_capacity() : 256;
        if (cap < 16) return;
        // Frame overhead: V73 + type + len = 6 bytes
        size_t max_payload = cap > 8 ? cap - 8 : 8;
        while (!host_tx_.empty() && state_ == State::Connected) {
            size_t n = std::min(max_payload, host_tx_.size());
            std::vector<uint8_t> f;
            f.reserve(6 + n);
            f.push_back('V'); f.push_back('7'); f.push_back('3'); f.push_back(3);
            f.push_back((uint8_t)((n >> 8) & 0xff));
            f.push_back((uint8_t)(n & 0xff));
            for (size_t i = 0; i < n; ++i) {
                f.push_back(host_tx_.front());
                host_tx_.pop_front();
            }
            if (hooks_.queue_tx) hooks_.queue_tx(f);
            report_buffer_locked();
            // One modem frame per tick to avoid flooding the TX queue.
            break;
        }
    }

    void service_loop() {
        while (running_) {
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto now = std::chrono::steady_clock::now();

                if (registered_pending_ && now >= registered_at_) {
                    registered_pending_ = false;
                    say_locked("REGISTERED");
                    say_locked("ENCRYPTION DISABLED");
                }
                if (have_clients_ && now >= next_alive_) {
                    say_locked("IAMALIVE");
                    next_alive_ = now + std::chrono::milliseconds(IAMALIVE_MS);
                }
                if (state_ == State::Calling && now >= next_retry_) {
                    attempt_connect_locked();
                }
                if (state_ == State::Connected)
                    pump_tx_locked();

                if (hooks_.is_ptt) {
                    bool ptt = hooks_.is_ptt();
                    if (ptt != last_ptt_) {
                        last_ptt_ = ptt;
                        say_locked(ptt ? "PTT ON" : "PTT OFF");
                    }
                }
                if (hooks_.is_busy) {
                    bool busy = hooks_.is_busy();
                    if (busy != last_busy_) {
                        last_busy_ = busy;
                        busy_announced_ = busy;
                        say_locked(busy ? "BUSY ON" : "BUSY OFF");
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
};
