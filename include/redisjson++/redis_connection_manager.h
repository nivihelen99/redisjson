#pragma once

#include "common_types.h" // For ClientConfig
#include "exceptions.h"      // For ConnectionException
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>
#include <functional> // For std::function
#include <thread>     // For health check thread
#include <atomic>     // For health status and control flags

namespace redisjson {

struct ConnectionStatsInternal {
    std::atomic<uint32_t> total_connections{0};
    std::atomic<uint32_t> active_connections{0};
    std::atomic<uint32_t> idle_connections{0};
    std::atomic<uint64_t> connection_errors{0};
};

struct ConnectionStats {
    uint32_t total_connections;
    uint32_t active_connections;
    uint32_t idle_connections;
    uint64_t connection_errors;
};

class RedisConnection {
public:
    RedisConnection(const std::string& host, int port, const std::string& password,
                    int database, std::chrono::milliseconds timeout_ms);
    ~RedisConnection();

    RedisConnection(const RedisConnection&) = delete;
    RedisConnection& operator=(const RedisConnection&) = delete;
    RedisConnection(RedisConnection&& other) noexcept;
    RedisConnection& operator=(RedisConnection&& other) noexcept;

    bool connect();
    void disconnect();
    bool is_connected() const;
    redisContext* get_context(); 

    redisReply* command(const char* format, ...);
    redisReply* command_argv(int argc, const char **argv, const size_t *argvlen);

    std::chrono::steady_clock::time_point last_used_time;
    bool ping(); 
    const std::string& get_last_error() const { return last_error_message_; }

    // Accessors for host and port for logging/identification
    const std::string& get_host() const { return host_; }
    int get_port() const { return port_; }

private:
    std::string host_;
    int port_;
    std::string password_;
    int database_;
    std::chrono::milliseconds connect_timeout_ms_;
    redisContext* context_ = nullptr;
    bool connected_ = false;
    std::string last_error_message_;

    bool authenticate();
    bool select_database();
};

class RedisConnectionManager {
public:
    explicit RedisConnectionManager(const ClientConfig& config);
    ~RedisConnectionManager();

    RedisConnectionManager(const RedisConnectionManager&) = delete;
    RedisConnectionManager& operator=(const RedisConnectionManager&) = delete;

    struct RedisConnectionDeleter; // Forward declaration
    using RedisConnectionPtr = std::unique_ptr<RedisConnection, RedisConnectionDeleter>;

    RedisConnectionPtr get_connection();
    void return_connection(RedisConnectionPtr conn); 
    void close_all_connections();

    bool is_healthy() const; 
    ConnectionStats get_stats() const;
    void set_health_check_interval(std::chrono::seconds interval); 

    void on_connection_lost(std::function<void(const std::string&)> callback);
    void on_connection_restored(std::function<void(const std::string&)> callback);

// Made public so RedisConnectionDeleter can be defined outside but still be a nested type if desired
// Custom deleter for RedisConnection to be used with std::unique_ptr
    struct RedisConnectionDeleter {
        RedisConnectionManager* manager_ptr_ = nullptr; 

        RedisConnectionDeleter(RedisConnectionManager* manager = nullptr) : manager_ptr_(manager) {}

        void operator()(RedisConnection* conn_ptr) const {
            // If manager_ptr_ is set, this RedisConnectionPtr was obtained from the manager
            // and should be returned to the manager's pool when it goes out of scope.
            // The manager's return_connection method will then decide whether to pool it
            // or delete it (e.g., if the manager is shutting down or the connection is invalid).
            if (manager_ptr_ && conn_ptr) {
                // Pass ownership of conn_ptr to a new RedisConnectionPtr,
                // which is then passed to return_connection.
                // The deleter for this new temporary RedisConnectionPtr is set to
                // RedisConnectionDeleter(nullptr). This means if return_connection
                // does not ultimately store this pointer (e.g., because the pool is full
                // or the manager is shutting down), this temporary unique_ptr will
                // simply delete conn_ptr when it goes out of scope at the end of the
                // return_connection call. It prevents re-queuing or double-management.
                // Passing manager_ptr_ ensures the repooled connection retains a valid deleter.
                manager_ptr_->return_connection(RedisConnectionPtr(conn_ptr, RedisConnectionDeleter(manager_ptr_)));
            } else if (conn_ptr) {
                // If there's no manager_ptr_, or if this is the deleter for the temporary
                // RedisConnectionPtr created above and return_connection decided not to keep it,
                // then the connection should be deleted.
                delete conn_ptr;
            }
            // The unique_ptr that this deleter instance belongs to has now completed its duty:
            // conn_ptr has either been handed off to manager_ptr_->return_connection() or deleted.
        }
    };

private:
    ClientConfig config_;
    // Changed type of pool_
    std::vector<RedisConnectionManager::RedisConnectionPtr> pool_; 
    std::queue<RedisConnection*> available_connections_; 
    std::mutex pool_mutex_;
    std::condition_variable condition_;
    ConnectionStatsInternal stats_; 

    std::atomic<bool> shutting_down_{false};
    std::atomic<bool> primary_healthy_{false}; 

    std::thread health_check_thread_;
    std::atomic<bool> run_health_checker_{false};
    std::chrono::seconds health_check_interval_ = std::chrono::seconds(5);
    std::chrono::seconds max_idle_time_before_ping_ = std::chrono::seconds(60); 
    void health_check_loop();
    bool check_primary_health(); 
    void maintain_pool_size(std::unique_lock<std::mutex>& lock); 

    std::function<void(const std::string&)> connection_lost_callback_;
    std::function<void(const std::string&)> connection_restored_callback_;

    void initialize_pool();
    // Declare new method and adjust/remove old one
    RedisConnectionManager::RedisConnectionPtr create_new_connection_ptr(const std::string& host, int port);
    // std::unique_ptr<RedisConnection> create_new_connection(const std::string& host, int port); // Kept for now, to be reviewed
};

} // namespace redisjson
