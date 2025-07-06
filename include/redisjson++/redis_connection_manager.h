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

// Forward declare ClientConfig if it's not included via redis_json_client.h or a common header
// struct ClientConfig;

// Internal struct with atomic members for thread-safe updates
struct ConnectionStatsInternal {
    std::atomic<uint32_t> total_connections{0};
    std::atomic<uint32_t> active_connections{0};
    std::atomic<uint32_t> idle_connections{0};
    std::atomic<uint64_t> connection_errors{0};
};

// Snapshot struct with plain types for returning by value
struct ConnectionStats {
    uint32_t total_connections;
    uint32_t active_connections;
    uint32_t idle_connections;
    uint64_t connection_errors;
};

// Represents a single connection to Redis
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
    redisContext* get_context(); // Be cautious with direct context exposure

    // Example: Execute a command safely
    redisReply* command(const char* format, ...);
    redisReply* command_argv(int argc, const char **argv, const size_t *argvlen);


    std::chrono::steady_clock::time_point last_used_time;
    bool ping(); // Send PING to check health
    const std::string& get_last_error() const { return last_error_message_; }

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

    // Connection Management
    // Forward declaration for the custom deleter
    struct RedisConnectionDeleter;
    using RedisConnectionPtr = std::unique_ptr<RedisConnection, RedisConnectionDeleter>;

    RedisConnectionPtr get_connection();
    void return_connection(std::unique_ptr<RedisConnection> conn); // Keep this for the deleter's use
    void close_all_connections();

    // Health Monitoring
    bool is_healthy() const; // Overall health of the pool / primary connection
    ConnectionStats get_stats() const;
    void set_health_check_interval(std::chrono::seconds interval); // 0 to disable

    // Failover Support (Basic stubs, full failover is complex)
    // void add_replica(const std::string& host, int port);
    // void remove_replica(const std::string& host, int port);
    // void enable_auto_failover(bool enabled);

    // Events (Callbacks)
    void on_connection_lost(std::function<void(const std::string& /*host:port*/)> callback);
    void on_connection_restored(std::function<void(const std::string& /*host:port*/)> callback);

private:
    ClientConfig config_;
    std::vector<std::unique_ptr<RedisConnection>> pool_;
    std::queue<RedisConnection*> available_connections_; // Pointers to connections in pool_
    std::mutex pool_mutex_;
    std::condition_variable condition_;
    ConnectionStatsInternal stats_; // Use internal struct for atomics

    std::atomic<bool> shutting_down_{false};
    std::atomic<bool> primary_healthy_{false}; // Simplified health status

    // Health checking (optional background thread)
    std::thread health_check_thread_;
    std::atomic<bool> run_health_checker_{false};
    std::chrono::seconds health_check_interval_ = std::chrono::seconds(5);
    std::chrono::seconds max_idle_time_before_ping_ = std::chrono::seconds(60); // Max idle time before pinging on get, can be configurable
    void health_check_loop();
    bool check_primary_health(); // Specific check for the main configured host/port
    void maintain_pool_size(std::unique_lock<std::mutex>& lock); // Helper to replace bad/idle connections

    // Event callbacks
    std::function<void(const std::string&)> connection_lost_callback_;
    std::function<void(const std::string&)> connection_restored_callback_;

    void initialize_pool();
    std::unique_ptr<RedisConnection> create_new_connection(const std::string& host, int port);

public: // Made public so RedisConnectionDeleter can be defined outside but still be a nested type if desired
    // Custom deleter for RedisConnection to be used with std::unique_ptr
    struct RedisConnectionDeleter {
        RedisConnectionManager* manager_ptr_ = nullptr; // Pointer to the manager to call return_connection

        RedisConnectionDeleter(RedisConnectionManager* manager = nullptr) : manager_ptr_(manager) {}

        void operator()(RedisConnection* conn_ptr) const {
            if (manager_ptr_ && conn_ptr) {
                // The return_connection method expects a unique_ptr.
                // We construct one here temporarily to pass to return_connection.
                // This assumes return_connection will handle the actual deletion or pooling.
                // If conn_ptr is already managed by a unique_ptr that is going out of scope,
                // this deleter is called. The unique_ptr `conn` given to return_connection
                // will then manage the lifetime according to return_connection's logic.
                manager_ptr_->return_connection(std::unique_ptr<RedisConnection>(conn_ptr));
            } else if (conn_ptr) {
                // If no manager is provided (e.g., standalone connection not from pool),
                // default to deleting it. This might be undesirable if connections should always be pooled.
                // Consider logging a warning or throwing an exception if manager_ptr_ is null
                // and the connection was expected to be from a pool.
                delete conn_ptr;
            }
        }
    };
};


} // namespace redisjson
