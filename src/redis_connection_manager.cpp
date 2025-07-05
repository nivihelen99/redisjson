#include "redisjson++/redis_connection_manager.h"
#include <stdexcept> // For runtime_error
#include <vector>    // For command_argv
#include <cstdarg>   // For va_list, va_start, va_end
#include <cstring>   // For strcmp

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
    std::unique_lock<std::mutex> lock(pool_mutex_);

    while (true) { // Loop to retry if a pooled connection is bad or other transient issues.
        if (shutting_down_) {
            throw ConnectionException("Connection manager is shutting down.");
        }

        // Wait if pool is empty and we can't create more connections or are shutting down
        condition_.wait(lock, [this] { // Added [this] capture
            return shutting_down_ ||
                   !available_connections_.empty() ||
                   ((stats_.active_connections + pool_.size()) < static_cast<size_t>(this->config_.connection_pool_size)); // Used this->config_
        });

        if (shutting_down_) { // Check again after wait
            throw ConnectionException("Connection manager is shutting down while waiting for a connection.");
        }

        std::unique_ptr<RedisConnection> conn_to_return;

        if (!available_connections_.empty()) {
            RedisConnection* raw_conn = available_connections_.front();
            available_connections_.pop();
            stats_.idle_connections--;

            auto it = std::find_if(pool_.begin(), pool_.end(),
                                   [&](const std::unique_ptr<RedisConnection>& p) { return p.get() == raw_conn; });

            if (it != pool_.end()) {
                conn_to_return = std::move(*it); // Transfer ownership from pool_
                pool_.erase(it); // Remove the (now empty) unique_ptr from pool_
            } else {
                // Should not happen if available_connections_ is consistent with pool_.
                // If it does, log an error, restore idle_connections_ count and retry.
                stats_.idle_connections++; // Correct the optimistic decrement.
                // Optionally log: "Connection from available queue not found in main pool. Retrying."
                continue; // Retry getting a connection.
            }

            // Health check for retrieved idle connection
            bool healthy = true;
            if (std::chrono::steady_clock::now() - conn_to_return->last_used_time > max_idle_time_before_ping_) {
                lock.unlock(); // Unlock while pinging
                healthy = conn_to_return->ping();
                lock.lock(); // Re-lock

                if (shutting_down_) { // Check after re-lock
                     if(conn_to_return) conn_to_return->disconnect(); // clean up
                     throw ConnectionException("Connection manager is shutting down during health check.");
                }
            }

            if (!healthy || !conn_to_return->is_connected()) {
                stats_.connection_errors++;
                stats_.total_connections--; // It was part of the pool, now it's gone
                conn_to_return.reset(); // Explicitly destroy bad connection
                continue; // Retry getting a connection from the start of the loop.
            }

            // If we reach here, conn_to_return is healthy and valid
            stats_.active_connections++;
            return conn_to_return;

        } else if ((stats_.active_connections + pool_.size()) < static_cast<size_t>(config_.connection_pool_size)) {
            // Pool is not full (total connections < max), create a new connection
            lock.unlock();
            std::unique_ptr<RedisConnection> new_conn = create_new_connection(config_.host, config_.port);
            lock.lock(); // Re-acquire lock

            if (shutting_down_) { // Check after re-acquiring lock
                if(new_conn) new_conn->disconnect();
                throw ConnectionException("Connection manager is shutting down during new connection creation.");
            }

            if (new_conn && new_conn->is_connected()) {
                stats_.total_connections++; // Increment when a new connection is successfully created
                stats_.active_connections++;
                return new_conn;
            } else {
                stats_.connection_errors++;
                // Failed to create new connection.
                // Depending on policy, we could retry in the loop, or throw.
                // Throwing for now, as continuous failure might indicate a bigger problem.
                throw ConnectionException("Failed to create new connection. Host: " + config_.host + ":" + std::to_string(config_.port));
            }
        } else {
            // This case should ideally not be reached if CV wait condition is correct and no timeouts.
            // It means available_connections_ is empty AND pool is full (cannot create more).
            // This implies all connections are active. The CV should wait.
            // If we are here, it might be due to a timeout on CV wait (not implemented yet) or a logic flaw.
            // For robustness, if this state is reached, throw.
            throw ConnectionException("No available connections and pool is at max capacity and cannot create more.");
        }
    } // End of while(true)
}


void RedisConnectionManager::return_connection(std::unique_ptr<RedisConnection> conn) {
    if (!conn) return;

    bool returned_to_pool = false;
    { // Scope for lock
        std::unique_lock<std::mutex> lock(pool_mutex_);
        stats_.active_connections--;

        if (shutting_down_) {
            conn->disconnect();
            // total_connections will be decremented outside lock if it wasn't returned
        } else if (conn->is_connected() && (stats_.active_connections + pool_.size()) < static_cast<size_t>(config_.connection_pool_size)) {
            // The condition `(stats_.active_connections + pool_.size()) < config_.connection_pool_size` ensures that
            // by adding this connection (which was active, now idle) back to the pool (pool_.size() will increment),
            // we don't exceed the total connection limit.
            // active_connections has already been decremented.
            // So, we check if (current active + current idle + 1 for this conn) <= limit
            // which is (stats_.active_connections + pool_.size() + 1) <= config_.connection_pool_size
            // Or, more simply, if pool_.size() < config_.connection_pool_size (max number of idle connections)
            // AND total connections (active + idle) < max_total_connections
            // The current model: pool_ stores idle. config_.connection_pool_size is max *total* connections.
            // So, if returning an active connection to idle:
            // New idle count = pool_.size() + 1
            // New active count = stats_.active_connections (already decremented)
            // Total = stats_.active_connections + pool_.size() + 1. This must be <= config_.connection_pool_size.
            if ((stats_.active_connections + pool_.size() + 1) <= static_cast<size_t>(config_.connection_pool_size) ) {
                available_connections_.push(conn.get());
                pool_.push_back(std::move(conn));
                stats_.idle_connections++;
                condition_.notify_one();
                returned_to_pool = true;
            } else {
                 // Pool is at capacity for total connections if this one were to be made idle.
                 // This implies it should be discarded.
                conn->disconnect();
                stats_.connection_errors++; // Or some other stat for "discarded due to pool full"
            }
        } else {
            // Connection is not healthy, or some other reason not to return.
            if (!conn->is_connected()) {
                stats_.connection_errors++;
            }
            conn->disconnect();
        }
    } // Lock released

    if (!returned_to_pool) {
        // If connection was not returned to pool (shutting down, unhealthy, or pool at capacity),
        // it means it's no longer managed by the pool, so decrement total_connections.
        // This needs to be done carefully, as total_connections is also modified by get_connection when creating new.
        // This logic assumes that any connection passed to return_connection was previously counted in total_connections.
        std::atomic_fetch_sub(&stats_.total_connections, 1u); // Ensure atomic operation
    }
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

redisjson::ConnectionStats RedisConnectionManager::get_stats() const {
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
            // More detailed check for idle connections in the pool:
            // Iterate `pool_` (which contains unique_ptrs to idle connections).
            // If a connection fails ping, remove it from `pool_` and `available_connections_`.
            // Then call maintain_pool_size.

            // Need to be careful with iterators if removing elements.
            // A common pattern: iterate backwards or use iterators correctly after erasure.
            for (auto it = pool_.begin(); it != pool_.end(); /* no increment here */) {
                if (shutting_down_ || !run_health_checker_) break;
                RedisConnection* conn_ptr = it->get();
                bool is_in_available_queue = false; // Check if conn_ptr is in available_connections_

                // To check if conn_ptr is in available_connections_, we'd have to iterate the queue.
                // This is inefficient. Assume all connections in pool_ are meant to be available if healthy.
                // The health_check_loop should primarily ensure that connections *in the pool* (idle ones) are healthy.

                // Let's refine: available_connections_ queue should be the source of truth for idle connections to check.
                // However, pool_ is the owner.
                // Iterate pool_. Ping each connection. If bad, remove from available_connections_ (if present) and from pool_.
                // This is complex due to available_connections_ storing raw pointers.

                // Simpler approach for health_check_loop:
                // Check a portion of idle connections or focus on very old ones.
                // For now, let's iterate `pool_` (which are all idle connections by current design).
                if (!(*it)->ping()) {
                    stats_.connection_errors++;
                    // This connection is bad. Remove it from available_connections_ queue first.
                    // This requires finding the raw pointer in the queue.
                    std::queue<RedisConnection*> new_available_connections;
                    while(!available_connections_.empty()){
                        RedisConnection* q_conn = available_connections_.front();
                        available_connections_.pop();
                        if(q_conn != conn_ptr){
                            new_available_connections.push(q_conn);
                        } else {
                            stats_.idle_connections--; // It was idle, now removed
                        }
                    }
                    available_connections_ = new_available_connections;

                    // Now remove from pool_
                    it = pool_.erase(it); // erase returns iterator to next element
                    stats_.total_connections--; // A connection is gone
                } else {
                    ++it; // Only increment if not erased
                }
            }
            // After checking and removing bad ones, try to replenish.
            maintain_pool_size(lock); // maintain_pool_size expects the lock to be held
        }


        // Wait for the interval or until notified (e.g., for shutdown)
        std::unique_lock<std::mutex> cv_lock(pool_mutex_); // Mutex for condition variable, different from the one passed to maintain_pool_size
        if (shutting_down_ || !run_health_checker_) break;
        condition_.wait_for(cv_lock, health_check_interval_, [this]{ return shutting_down_.load() || !run_health_checker_.load(); });
    }
}

// Helper function to ensure pool maintains a minimum number of healthy connections
// up to config_.connection_pool_size, if specified.
// Assumes pool_mutex_ is already locked by the caller.
void RedisConnectionManager::maintain_pool_size(std::unique_lock<std::mutex>& lock) {
    if (shutting_down_ || !run_health_checker_) return;

    // Example: Maintain a minimum number of idle connections, e.g., config_.connection_pool_size / 2 or a fixed number.
    // For now, let's try to fill up to config_.connection_pool_size if below.
    // This is aggressive and might not be ideal if connections are expensive or traffic is low.
    // A more nuanced strategy would consider min_idle, max_idle, etc.

    size_t desired_pool_size = static_cast<size_t>(config_.connection_pool_size);
    // This is max total. If pool_ stores idle, then desired idle might be smaller.
    // Let's assume for now we want to keep the pool of *idle* connections full if possible.
    // No, config_.connection_pool_size is the *total* (active + idle).
    // So, we can only add new idle connections if (active + current_idle) < max_total.

    while ((stats_.active_connections + pool_.size()) < desired_pool_size && !shutting_down_) {
        // Unlock mutex while creating a new connection to avoid deadlock / long lock hold.
        // This is tricky as the lock was passed in.
        // Alternative: maintain_pool_size creates connections without releasing the main lock if create_new_connection is fast.
        // For now, let's assume create_new_connection is acceptable to call with lock held, or refactor create_new_connection.
        // Given create_new_connection involves network I/O, it's better to release.
        // This means maintain_pool_size cannot take unique_lock by reference if it unlocks/relocks.
        // Let's simplify: maintain_pool_size will just try to add one connection if needed and possible.

        // This function is called from health_check_loop, which holds its own lock.
        // The lock passed here is that same lock.

        if ((stats_.active_connections + pool_.size()) >= desired_pool_size) {
            break; // Pool is full enough
        }

        // Temporarily release lock to create connection
        lock.unlock();
        auto conn = create_new_connection(config_.host, config_.port);
        lock.lock(); // Re-acquire the lock

        if (shutting_down_) break; // Check again after re-acquiring lock

        if (conn && conn->is_connected()) {
            available_connections_.push(conn.get());
            pool_.push_back(std::move(conn));
            stats_.idle_connections++;
            stats_.total_connections++;
            condition_.notify_one(); // A new connection is available
        } else {
            stats_.connection_errors++;
            // Failed to create, maybe stop trying for a bit in this cycle
            break;
        }
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
