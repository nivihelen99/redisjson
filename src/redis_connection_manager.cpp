#include "redisjson++/redis_connection_manager.h"
#include <stdexcept> // For runtime_error
#include <vector>    // For command_argv
#include <cstdarg>   // For va_list, va_start, va_end

namespace redisjson {

// --- RedisConnection Implementation ---
RedisConnection::RedisConnection(const std::string& host, int port, const std::string& password,
                                 int database, std::chrono::milliseconds timeout_ms)
    : host_(host), port_(port), password_(password), database_(database),
      connect_timeout_ms_(timeout_ms), context_(nullptr), connected_(false),
      last_used_time(std::chrono::steady_clock::now()) {}

RedisConnection::~RedisConnection() {
    disconnect();
}

RedisConnection::RedisConnection(RedisConnection&& other) noexcept
    : host_(std::move(other.host_)),
      port_(other.port_),
      password_(std::move(other.password_)),
      database_(other.database_),
      connect_timeout_ms_(other.connect_timeout_ms_),
      context_(other.context_),
      connected_(other.connected_),
      last_used_time(other.last_used_time) {
    other.context_ = nullptr; // Prevent double free
    other.connected_ = false;
}

RedisConnection& RedisConnection::operator=(RedisConnection&& other) noexcept {
    if (this != &other) {
        disconnect(); // Disconnect existing connection first

        host_ = std::move(other.host_);
        port_ = other.port_;
        password_ = std::move(other.password_);
        database_ = other.database_;
        connect_timeout_ms_ = other.connect_timeout_ms_;
        context_ = other.context_;
        connected_ = other.connected_;
        last_used_time = other.last_used_time;

        other.context_ = nullptr;
        other.connected_ = false;
    }
    return *this;
}

bool RedisConnection::connect() {
    if (connected_) return true;

    struct timeval tv;
    tv.tv_sec = connect_timeout_ms_.count() / 1000;
    tv.tv_usec = (connect_timeout_ms_.count() % 1000) * 1000;

    context_ = redisConnectWithTimeout(host_.c_str(), port_, tv);
    if (context_ == nullptr || context_->err) {
        if (context_) {
            // Optional: Log context_->errstr
            redisFree(context_);
            context_ = nullptr;
        }
        connected_ = false;
        return false;
    }

    // Set socket timeout for read/write operations (separate from connect timeout)
    // This is crucial to prevent indefinite blocking on commands.
    // Using the same timeout as connect for now, but could be different.
    if (redisSetTimeout(context_, tv) != REDIS_OK) {
        // Optional: Log error
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    if (!authenticate()) {
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    if (!select_database()) {
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    connected_ = true;
    last_used_time = std::chrono::steady_clock::now();
    return true;
}

void RedisConnection::disconnect() {
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
    connected_ = false;
}

bool RedisConnection::is_connected() const {
    return connected_ && context_ != nullptr && context_->err == 0;
}

redisContext* RedisConnection::get_context() {
    return context_;
}

bool RedisConnection::authenticate() {
    if (password_.empty()) {
        return true; // No password needed
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "AUTH %s", password_.c_str()));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        // Optional: Log reply->str if error
        if (reply) freeReplyObject(reply);
        return false;
    }
    freeReplyObject(reply);
    return true;
}

bool RedisConnection::select_database() {
    if (database_ == 0) { // Default DB, no need to select explicitly unless changed from 0
        return true;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "SELECT %d", database_));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        // Optional: Log reply->str if error
        if (reply) freeReplyObject(reply);
        return false;
    }
    freeReplyObject(reply);
    return true;
}

bool RedisConnection::ping() {
    if (!is_connected()) {
        return false;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "PING"));
    if (reply == nullptr) { // Indicates connection error (e.g. broken pipe)
        connected_ = false; // Mark as disconnected
        return false;
    }
    bool success = (reply->type == REDIS_REPLY_STATUS && (strcmp(reply->str, "PONG") == 0 || strcmp(reply->str, "OK") == 0));
    if (reply->type == REDIS_REPLY_ERROR) { // Explicit error from Redis
         connected_ = false; // Could be auth error after reconnect etc.
    }
    freeReplyObject(reply);
    if (success) last_used_time = std::chrono::steady_clock::now();
    return success;
}


redisReply* RedisConnection::command(const char* format, ...) {
    if (!is_connected()) {
        // Attempt a reconnect ONCE. More sophisticated retry should be in RedisJSONClient.
        // Or, this layer simply reports failure. For a pool, connection might be discarded.
        // For now, let's assume get_connection() from manager provides a live one or throws.
        // So, if is_connected() is false here, it's a potentially broken connection.
        return nullptr;
    }
    va_list ap;
    va_start(ap, format);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(context_, format, ap));
    va_end(ap);

    if (reply == nullptr) { // Serious connection error (e.g., broken pipe, timeout)
        connected_ = false; // Mark connection as potentially bad
    } else {
        last_used_time = std::chrono::steady_clock::now();
    }
    return reply;
}

redisReply* RedisConnection::command_argv(int argc, const char **argv, const size_t *argvlen) {
    if (!is_connected()) {
        return nullptr;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(context_, argc, argv, argvlen));
    if (reply == nullptr) {
        connected_ = false;
    } else {
        last_used_time = std::chrono::steady_clock::now();
    }
    return reply;
}


// --- RedisConnectionManager Implementation ---
RedisConnectionManager::RedisConnectionManager(const ClientConfig& config) : config_(config) {
    initialize_pool();
    if (config_.connection_pool_size > 0 && health_check_interval_.count() > 0) {
        run_health_checker_ = true;
        health_check_thread_ = std::thread(&RedisConnectionManager::health_check_loop, this);
    }
}

RedisConnectionManager::~RedisConnectionManager() {
    shutting_down_ = true;
    if (run_health_checker_) {
        run_health_checker_ = false; // Signal health checker to stop
        condition_.notify_all(); // Wake up health checker if it's waiting
        if (health_check_thread_.joinable()) {
            health_check_thread_.join();
        }
    }
    close_all_connections();
}

void RedisConnectionManager::initialize_pool() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    for (int i = 0; i < config_.connection_pool_size; ++i) {
        auto conn = create_new_connection(config_.host, config_.port);
        if (conn->is_connected()) {
            pool_.push_back(std::move(conn));
            available_connections_.push(pool_.back().get());
            stats_.idle_connections++;
            if (i == 0) primary_healthy_ = true; // Assume healthy if first one connects
        } else {
            stats_.connection_errors++;
            // Log error or throw if min connections not met? For now, just count error.
            if (i == 0) primary_healthy_ = false;
        }
        stats_.total_connections++;
    }
}

std::unique_ptr<RedisConnection> RedisConnectionManager::create_new_connection(const std::string& host, int port) {
    // Note: config_ might contain primary host/port. This fn could take specific host/port for replicas.
    auto conn = std::make_unique<RedisConnection>(host, port, config_.password, config_.database, config_.timeout);
    conn->connect(); // Attempt to connect
    return conn;
}


std::unique_ptr<RedisConnection> RedisConnectionManager::get_connection() {
    // Simplified: Always create a new connection. Pooling for reuse is bypassed for now.
    // Proper pooling with unique_ptr ownership requires careful handling (e.g. custom deleters
    // or revising how available_connections_ and pool_ interact with unique_ptr).
    if (shutting_down_) {
        throw ConnectionException("Connection manager is shutting down.");
    }

    auto conn = create_new_connection(config_.host, config_.port);
    if (!conn->is_connected()) {
        stats_.connection_errors++;
        throw ConnectionException("Failed to create new connection for get_connection. Host: " + config_.host + ":" + std::to_string(config_.port));
    }

    // Increment active connections, assuming this new one is active.
    // Total connections might also be tracked if we want to limit overall connections.
    // For now, this bypasses detailed pool stat tracking.
    stats_.active_connections++;
    // stats_.total_connections++; // If we track total live connections including those not in "pool_"

    return conn; // Ownership is transferred to the caller.
}


void RedisConnectionManager::return_connection(std::unique_ptr<RedisConnection> conn) {
    if (!conn) return;

    // Simplified: Connection is not returned to a pool for reuse.
    // It will be destroyed when `conn` (the unique_ptr) goes out of scope.
    // The `initialize_pool` creates some initial connections, but they are not currently reused by this get/return mechanism.
    // A real implementation would add `conn` back to `available_connections_` if it's healthy and there's space.

    std::unique_lock<std::mutex> lock(pool_mutex_); // Lock for stats update
    stats_.active_connections--;

    // If we were to implement pooling here:
    // if (!shutting_down_ && pool_.size() < static_cast<size_t>(config_.connection_pool_size)) {
    //     if (conn->ping()) { // Check health before pooling
    //         pool_.push_back(std::move(conn)); // Add to main ownership vector
    //         available_connections_.push(pool_.back().get()); // Add raw ptr to available queue
    //         stats_.idle_connections++;
    //         condition_.notify_one();
    //         return; // Successfully returned to pool
    //     } else {
    //         stats_.connection_errors++;
    //         // Bad connection, don't pool it. Falls through to be destroyed.
    //     }
    // }
    // If not returned to pool (e.g. pool full, shutting down, or bad conn), it's destroyed by unique_ptr.
    // For this simplified version, it always falls through and gets destroyed by unique_ptr.
    // We just decrement active_connections.
}


void RedisConnectionManager::close_all_connections() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    shutting_down_ = true; // Ensure this is set if not already

    while (!available_connections_.empty()) {
        available_connections_.pop();
    }
    // This clears the queue of raw pointers. The unique_ptrs in pool_ will handle actual disconnection.
    pool_.clear(); // This will call destructors of all unique_ptrs, which call RedisConnection destructors.
    stats_.active_connections = 0;
    stats_.idle_connections = 0;
    stats_.total_connections = 0; // Or should reflect max size if that's meaningful
    primary_healthy_ = false;
}

bool RedisConnectionManager::is_healthy() const {
    // Could be more sophisticated: e.g., at least one connection in pool is healthy,
    // or primary configured host is reachable.
    return primary_healthy_;
}

ConnectionStats RedisConnectionManager::get_stats() const {
    // Access to stats members should be atomic or protected if modified by multiple threads
    // (e.g. health checker). `std::atomic` is used, so direct return is fine.
    return stats_;
}

void RedisConnectionManager::set_health_check_interval(std::chrono::seconds interval) {
    // This might require restarting the health check thread if interval changes significantly
    // or stopping/starting it. For simplicity, assume it's set at construction.
    // If called later, its effect on running thread might be delayed or ignored.
    health_check_interval_ = interval;
    if (interval.count() <= 0 && run_health_checker_) {
        run_health_checker_ = false; // Signal to stop
    } else if (interval.count() > 0 && !run_health_checker_ && !health_check_thread_.joinable() && config_.connection_pool_size > 0) {
        // Start it if it wasn't running and interval is now positive
        run_health_checker_ = true;
        shutting_down_ = false; // If it was stopped due to shutdown, reset
        health_check_thread_ = std::thread(&RedisConnectionManager::health_check_loop, this);
    }
}

void RedisConnectionManager::on_connection_lost(std::function<void(const std::string&)> callback) {
    connection_lost_callback_ = callback;
}

void RedisConnectionManager::on_connection_restored(std::function<void(const std::string&)> callback) {
    connection_restored_callback_ = callback;
}

void RedisConnectionManager::health_check_loop() {
    while (run_health_checker_ && !shutting_down_) {
        // Perform health check
        bool previously_healthy = primary_healthy_.load();
        bool currently_healthy = check_primary_health(); // Checks main connection

        if (currently_healthy && !previously_healthy && connection_restored_callback_) {
            connection_restored_callback_(config_.host + ":" + std::to_string(config_.port));
        } else if (!currently_healthy && previously_healthy && connection_lost_callback_) {
            connection_lost_callback_(config_.host + ":" + std::to_string(config_.port));
        }
        primary_healthy_ = currently_healthy;

        // Check all idle connections in the pool too
        { // Scope for lock
            std::unique_lock<std::mutex> lock(pool_mutex_);
            if (shutting_down_ || !run_health_checker_) break;

            // Iterate over available_connections_ queue (pointers) and ping them.
            // If a connection in available_connections_ is bad, it should be removed
            // from available_connections_ and its owning unique_ptr in pool_ reset/removed.
            // This is complex as it modifies queue while iterating.
            // Easier: iterate `pool_` itself. If a unique_ptr in `pool_` points to a bad connection,
            // reset that unique_ptr. Then rebuild `available_connections_` queue.
            // Or, if a connection is found bad, remove its raw ptr from `available_connections_`
            // AND remove its owning `unique_ptr` from `pool_`.

            // Simplified health check for idle connections:
            // Iterate `pool_`. If `conn` is present and not in `available_connections_` (i.e., active or bad), skip.
            // If `conn` is in `available_connections_`, try ping. If bad, remove from pool and available.
            // This needs careful synchronization.

            // For now, health_check_loop only focuses on primary_healthy_ status via check_primary_health().
            // Individual connections are checked more lazily (e.g. on get_connection or by more detailed health check).
        }

        // Wait for the interval or until notified (e.g., for shutdown)
        std::unique_lock<std::mutex> cv_lock(pool_mutex_); // Mutex for condition variable
        if (shutting_down_ || !run_health_checker_) break;
        condition_.wait_for(cv_lock, health_check_interval_, [this]{ return shutting_down_.load() || !run_health_checker_.load(); });
    }
}

bool RedisConnectionManager::check_primary_health() {
    // Tries to establish a new, temporary connection to check primary server health.
    // Does not use pooled connections for this check itself to avoid taking one.
    RedisConnection temp_conn(config_.host, config_.port, config_.password, config_.database, std::chrono::milliseconds(1000)); // Short timeout for health check
    if (temp_conn.connect()) {
        if (temp_conn.ping()) {
            temp_conn.disconnect();
            return true;
        }
        temp_conn.disconnect();
    }
    return false;
}


} // namespace redisjson
