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
            if (manager_ptr_ && conn_ptr) {
                // This was changed to expect RedisConnectionPtr
                // However, the raw pointer is passed to the deleter.
                // The responsibility of returning to pool is complex if deleter itself tries to call return_connection
                // with a new unique_ptr. Simpler: just delete if manager_ptr_ is null, otherwise do nothing
                // as return_connection (which now takes RedisConnectionPtr) should be the one putting it back.
                // For now, let's assume the manager handles the return, and deleter just deletes if no manager.
                // This part of logic might need further review based on how RedisConnectionPtr is used.
                // If a RedisConnectionPtr goes out of scope, its deleter is called.
                // If it was obtained from get_connection, it should have been returned via return_connection.
                // If return_connection then moves it to pool_, the deleter on the original ptr shouldn't re-delete.
                // This implies that return_connection should ideally take ownership and potentially reset the original ptr
                // or the pool should store raw pointers if the manager exclusively owns them.
                // Given the current structure with unique_ptr in pool, this is tricky.
                // Let's revert to the original idea: if manager_ptr is set, it means the connection *might* be from pool.
                // The original logic was manager_ptr_->return_connection(RedisConnectionPtr(conn_ptr));
                // which is problematic if conn_ptr is already managed by the unique_ptr whose deleter this is.
                // Safest for now: if manager_ptr is null, delete. Otherwise, assume manager handles it.
                // This implies connections taken from pool and not explicitly returned might leak if not handled.
                // This is a common unique_ptr with custom deleter challenge.
                // Let's stick to the idea that the deleter's job is primarily to delete if the object
                // is not meant to be returned to a pool, or if the pool itself is being destroyed.
                // The current design: get_connection() returns a RedisConnectionPtr.
                // When this ptr goes out of scope, this deleter is called.
                // If the user hasn't called return_connection(), the connection should be closed and deleted.
                // If they *have* called return_connection(), then that function took ownership (moved it into pool_),
                // so the original ptr should be null or its deleter shouldn't try to return/delete again.
                // This suggests the manager_ptr_ in deleter might be for a different purpose or needs rethinking.

                // Revised logic for deleter:
                // If the connection is still valid and a manager exists,
                // it implies the unique_ptr is being destroyed without being returned to the pool.
                // In this case, we should probably just ensure it's disconnected and deleted.
                // The manager's return_connection is for explicitly giving it back.
                if (conn_ptr) { // Check if conn_ptr is not null
                     // If manager_ptr_ is set, it suggests it *could* have been from the pool.
                     // However, if this deleter is called, it means the unique_ptr owning conn_ptr is dying.
                     // If it wasn't returned, it's now "orphaned" from the pool's perspective.
                     // So, just delete it. The pool manages its own unique_ptrs.
                    delete conn_ptr;
                }

            } else if (conn_ptr) {
                delete conn_ptr;
            }
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
