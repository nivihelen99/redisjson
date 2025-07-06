#include "redisjson++/redis_connection_manager.h"
#include <stdexcept> // For runtime_error
#include <vector>    // For command_argv
#include <cstdarg>   // For va_list, va_start, va_end
#include <cstring>   // For strcmp
#include <algorithm> // For std::find_if

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
    last_error_message_.clear(); // Clear previous error

    struct timeval tv_timeout;
    tv_timeout.tv_sec = connect_timeout_ms_.count() / 1000;
    tv_timeout.tv_usec = (connect_timeout_ms_.count() % 1000) * 1000;

    context_ = redisConnectWithTimeout(host_.c_str(), port_, tv_timeout);

    if (context_ == nullptr || context_->err) {
        if (context_) {
            last_error_message_ = "hiredis context error (redisConnectWithTimeout): " + std::string(context_->errstr ? context_->errstr : "Unknown error") + " (code: " + std::to_string(context_->err) + ")";
            redisFree(context_);
            context_ = nullptr;
        } else {
            last_error_message_ = "hiredis failed to allocate context (redisConnectWithTimeout)";
        }
        connected_ = false;
        return false;
    }

    if (redisSetTimeout(context_, tv_timeout) != REDIS_OK) {
        if (context_->errstr) {
            last_error_message_ = "redisSetTimeout failed: " + std::string(context_->errstr) + " (code: " + std::to_string(context_->err) + ")";
        } else {
            last_error_message_ = "redisSetTimeout failed with no specific error string (code: " + std::to_string(context_->err) + ")";
        }
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    if (!authenticate()) {
       if (last_error_message_.empty()) last_error_message_ = "Authentication failed";
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    if (!select_database()) {
       if (last_error_message_.empty()) last_error_message_ = "Database selection failed";
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
        return true;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "AUTH %s", password_.c_str()));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
       if (reply && reply->str) {
           last_error_message_ = "Authentication failed: " + std::string(reply->str);
       } else if (context_->errstr && context_->err != 0) {
           last_error_message_ = "Authentication failed: " + std::string(context_->errstr) + " (code: " + std::to_string(context_->err) + ")";
       } else {
           last_error_message_ = "Authentication failed: No reply or unknown error.";
       }
        if (reply) freeReplyObject(reply);
        return false;
    }
    freeReplyObject(reply);
    return true;
}

bool RedisConnection::select_database() {
    if (database_ == 0) {
        return true;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "SELECT %d", database_));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
       if (reply && reply->str) {
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: " + std::string(reply->str);
       } else if (context_->errstr && context_->err != 0) {
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: " + std::string(context_->errstr) + " (code: " + std::to_string(context_->err) + ")";
       } else {
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: No reply or unknown error.";
       }
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
    if (reply == nullptr) {
        connected_ = false;
        return false;
    }
    bool success = (reply->type == REDIS_REPLY_STATUS && (strcmp(reply->str, "PONG") == 0 || strcmp(reply->str, "OK") == 0));
    if (reply->type == REDIS_REPLY_ERROR) {
         connected_ = false;
    }
    freeReplyObject(reply);
    if (success) last_used_time = std::chrono::steady_clock::now();
    return success;
}

redisReply* RedisConnection::command(const char* format, ...) {
    if (!is_connected()) {
        return nullptr;
    }
    va_list ap;
    va_start(ap, format);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(context_, format, ap));
    va_end(ap);

    if (reply == nullptr) {
        connected_ = false;
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
        run_health_checker_ = false;
        condition_.notify_all();
        if (health_check_thread_.joinable()) {
            health_check_thread_.join();
        }
    }
    close_all_connections();
}

// Changed to create_new_connection_ptr and return RedisConnectionPtr
RedisConnectionManager::RedisConnectionPtr RedisConnectionManager::create_new_connection_ptr(const std::string& host, int port) {
    auto conn_raw = new RedisConnection(host, port, config_.password, config_.database, config_.timeout);
    conn_raw->connect(); // Attempt to connect
    return RedisConnectionPtr(conn_raw, RedisConnectionDeleter(this));
}

void RedisConnectionManager::initialize_pool() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    for (int i = 0; i < config_.connection_pool_size; ++i) {
        RedisConnection* conn_raw = new RedisConnection(config_.host, config_.port, config_.password, config_.database, config_.timeout);
        bool connected_successfully = false;

        if (conn_raw) { // Check if allocation succeeded
            connected_successfully = conn_raw->connect();
        }

        if (connected_successfully) {
            RedisConnectionManager::RedisConnectionPtr conn(conn_raw, RedisConnectionDeleter(this));
            available_connections_.push(conn.get());
            pool_.push_back(std::move(conn));
            stats_.idle_connections++;
            stats_.total_connections++; // Increment for successfully created and pooled connections
            if (pool_.size() == 1 && i == 0) primary_healthy_ = true; // If it's the first potential primary and successfully pooled
        } else {
            stats_.connection_errors++;
            if (conn_raw) {
                delete conn_raw; // Directly delete failed connection
            }
            if (i == 0) primary_healthy_ = false; // If the first attempt failed
            // Do not increment total_connections for connections that failed to initialize
        }
    }
}

RedisConnectionManager::RedisConnectionPtr RedisConnectionManager::get_connection() {
    std::unique_lock<std::mutex> lock(pool_mutex_);

    while (true) {
        if (shutting_down_) {
            throw ConnectionException("Connection manager is shutting down.");
        }

        condition_.wait(lock, [this] {
            return shutting_down_ ||
                   !available_connections_.empty() ||
                   ((stats_.active_connections + pool_.size()) < static_cast<size_t>(this->config_.connection_pool_size));
        });

        if (shutting_down_) {
            throw ConnectionException("Connection manager is shutting down while waiting for a connection.");
        }

        // Change type of conn_to_return
        RedisConnectionManager::RedisConnectionPtr conn_to_return;

        if (!available_connections_.empty()) {
            RedisConnection* raw_conn = available_connections_.front();
            available_connections_.pop();
            stats_.idle_connections--;

            // Update lambda to accept const RedisConnectionManager::RedisConnectionPtr&
            auto it = std::find_if(pool_.begin(), pool_.end(),
                                   [&](const RedisConnectionManager::RedisConnectionPtr& p) { return p.get() == raw_conn; });

            if (it != pool_.end()) {
                conn_to_return = std::move(*it);
                pool_.erase(it);
            } else {
                stats_.idle_connections++;
                continue;
            }

            bool healthy = true;
            if (conn_to_return && std::chrono::steady_clock::now() - conn_to_return->last_used_time > max_idle_time_before_ping_) {
                lock.unlock();
                healthy = conn_to_return->ping();
                lock.lock();

                if (shutting_down_) {
                     if(conn_to_return) conn_to_return->disconnect();
                     throw ConnectionException("Connection manager is shutting down during health check.");
                }
            }

            if (!healthy || !conn_to_return || !conn_to_return->is_connected()) {
                stats_.connection_errors++;
                stats_.total_connections--;
                // conn_to_return (RedisConnectionPtr) will handle its own deletion via deleter
                conn_to_return.reset(); 
                continue;
            }
            stats_.active_connections++;
            return conn_to_return; // Already RedisConnectionPtr

        } else if ((stats_.active_connections + pool_.size()) < static_cast<size_t>(config_.connection_pool_size)) {
            lock.unlock();
            // Use create_new_connection_ptr
            RedisConnectionManager::RedisConnectionPtr new_conn = create_new_connection_ptr(config_.host, config_.port);
            lock.lock();

            if (shutting_down_) {
                if(new_conn) new_conn->disconnect();
                throw ConnectionException("Connection manager is shutting down during new connection creation.");
            }

            if (new_conn && new_conn->is_connected()) {
                stats_.total_connections++;
                stats_.active_connections++;
                return new_conn; // Already RedisConnectionPtr
            } else {
                stats_.connection_errors++;
               std::string error_detail = "Unknown connection failure.";
               if (new_conn) {
                   error_detail = new_conn->get_last_error();
               } else {
                   error_detail = "Failed to allocate RedisConnection object via create_new_connection_ptr.";
               }
               // new_conn (RedisConnectionPtr) will handle its own deletion if not returned
               throw ConnectionException("Failed to create new connection to " + config_.host + ":" + std::to_string(config_.port) + ". Detail: " + error_detail);
            }
        } else {
            throw ConnectionException("No available connections and pool is at max capacity and cannot create more.");
        }
    }
}

void RedisConnectionManager::return_connection(RedisConnectionManager::RedisConnectionPtr conn) {
    if (!conn) return;

    bool returned_to_pool = false;
    { 
        std::unique_lock<std::mutex> lock(pool_mutex_);
        stats_.active_connections--;

        if (shutting_down_) {
            conn->disconnect();
        } else if (conn->is_connected() && (stats_.active_connections + pool_.size()) < static_cast<size_t>(config_.connection_pool_size)) {
            if ((stats_.active_connections + pool_.size() + 1) <= static_cast<size_t>(config_.connection_pool_size) ) {
                available_connections_.push(conn.get());
                pool_.push_back(std::move(conn)); // conn is already RedisConnectionPtr
                stats_.idle_connections++;
                condition_.notify_one();
                returned_to_pool = true;
            } else {
                conn->disconnect();
                stats_.connection_errors++; 
            }
        } else {
            if (conn && !conn->is_connected()) { // Check conn before calling is_connected
                stats_.connection_errors++;
            }
            if(conn) conn->disconnect(); // Check conn before calling disconnect
        }
    } 

    if (!returned_to_pool && conn) { // If conn was not moved (and not null)
        // It means it's no longer managed by the pool, so decrement total_connections.
        // The RedisConnectionPtr's deleter will handle actual deletion.
        std::atomic_fetch_sub(&stats_.total_connections, 1u); 
    } else if (!returned_to_pool && !conn) {
        // This case implies conn was already null or became null (e.g. moved then reset elsewhere)
        // and wasn't returned. If it was counted, it should be decremented.
        // This path is less clear without full context of how conn could be null AND not returned.
        // Assuming if conn is null here and not returned, its count was already handled or never added.
    }
}

void RedisConnectionManager::close_all_connections() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    shutting_down_ = true; 

    while (!available_connections_.empty()) {
        available_connections_.pop();
    }
    pool_.clear(); 
    stats_.active_connections = 0;
    stats_.idle_connections = 0;
    stats_.total_connections = 0; 
    primary_healthy_ = false;
}

bool RedisConnectionManager::is_healthy() const {
    return primary_healthy_;
}

redisjson::ConnectionStats RedisConnectionManager::get_stats() const {
    return {
        stats_.total_connections.load(std::memory_order_relaxed),
        stats_.active_connections.load(std::memory_order_relaxed),
        stats_.idle_connections.load(std::memory_order_relaxed),
        stats_.connection_errors.load(std::memory_order_relaxed)
    };
}

void RedisConnectionManager::set_health_check_interval(std::chrono::seconds interval) {
    health_check_interval_ = interval;
    if (interval.count() <= 0 && run_health_checker_) {
        run_health_checker_ = false; 
    } else if (interval.count() > 0 && !run_health_checker_ && !health_check_thread_.joinable() && config_.connection_pool_size > 0) {
        run_health_checker_ = true;
        shutting_down_ = false; 
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
    std::vector<RedisConnectionManager::RedisConnectionPtr> connections_to_remove_outside_lock;

    while (run_health_checker_ && !shutting_down_) {
        bool previously_healthy = primary_healthy_.load();
        bool currently_healthy = check_primary_health();

        if (currently_healthy && !previously_healthy && connection_restored_callback_) {
            connection_restored_callback_(config_.host + ":" + std::to_string(config_.port));
        } else if (!currently_healthy && previously_healthy && connection_lost_callback_) {
            connection_lost_callback_(config_.host + ":" + std::to_string(config_.port));
        }
        primary_healthy_ = currently_healthy;

        { // Start of critical section with pool_mutex_
            std::unique_lock<std::mutex> lock(pool_mutex_);
            if (shutting_down_ || !run_health_checker_) break;

            for (auto it = pool_.begin(); it != pool_.end(); ) {
                if (shutting_down_ || !run_health_checker_) break;
                
                bool is_bad = false;
                if (it->get()) { // Check if pointer inside unique_ptr is valid
                    if (!(*it)->ping()) { // ping() might fail if connection is truly bad
                        is_bad = true;
                    }
                } else { 
                    // This case (nullptr in pool_) should ideally not happen if pool is managed correctly.
                    is_bad = true;
                }

                if (is_bad) {
                    stats_.connection_errors++;
                    RedisConnection* conn_raw_ptr = it->get();
                    
                    // Remove from available_connections_ queue if present
                    std::queue<RedisConnection*> new_available_connections_q;
                    while(!available_connections_.empty()){
                        RedisConnection* q_conn = available_connections_.front();
                        available_connections_.pop();
                        if(q_conn != conn_raw_ptr){
                            new_available_connections_q.push(q_conn);
                        } else {
                            // If found in available_connections_, it was idle.
                            stats_.idle_connections--; 
                        }
                    }
                    available_connections_ = new_available_connections_q;
                    
                    // Move the RedisConnectionPtr to the temporary vector for destruction outside the lock.
                    connections_to_remove_outside_lock.push_back(std::move(*it));
                    it = pool_.erase(it); // Erase the (now empty) unique_ptr from pool_ vector.
                                          // This does not call the deleter of the connection itself yet.
                    stats_.total_connections--; // Decrement total connections count.
                } else {
                    ++it; 
                }
            }
            maintain_pool_size(lock); // This might add new connections to the pool.
                                      // Lock is passed and potentially unlocked/relocked inside.
        } // End of critical section, pool_mutex_ is released by 'lock' going out of scope.

        // Destroy connections that were marked for removal.
        // Their deleters will call RedisConnectionManager::return_connection.
        // return_connection will acquire pool_mutex_ without deadlocking,
        // and will ultimately delete the RedisConnection object because it's
        // not being returned to the pool (it's already been removed).
        if (!connections_to_remove_outside_lock.empty()) {
            connections_to_remove_outside_lock.clear();
        }

        // Wait for the next health check interval or shutdown signal.
        // This re-acquires the mutex for the condition variable.
        std::unique_lock<std::mutex> cv_lock(pool_mutex_); 
        if (shutting_down_ || !run_health_checker_) break;
        condition_.wait_for(cv_lock, health_check_interval_, [this]{ return shutting_down_.load() || !run_health_checker_.load(); });
    }
}

void RedisConnectionManager::maintain_pool_size(std::unique_lock<std::mutex>& lock) {
    if (shutting_down_ || !run_health_checker_) return;

    size_t desired_pool_size = static_cast<size_t>(config_.connection_pool_size);

    while ((stats_.active_connections + pool_.size()) < desired_pool_size && !shutting_down_) {
        if ((stats_.active_connections + pool_.size()) >= desired_pool_size) {
            break; 
        }

        lock.unlock();
        // Use create_new_connection_ptr
        RedisConnectionManager::RedisConnectionPtr conn = create_new_connection_ptr(config_.host, config_.port);
        lock.lock(); 

        if (shutting_down_) break; 

        if (conn && conn->is_connected()) {
            available_connections_.push(conn.get());
            pool_.push_back(std::move(conn)); // conn is RedisConnectionPtr
            stats_.idle_connections++;
            stats_.total_connections++;
            condition_.notify_one(); 
        } else {
            stats_.connection_errors++;
            break;
        }
    }
}

bool RedisConnectionManager::check_primary_health() {
    RedisConnection temp_conn(config_.host, config_.port, config_.password, config_.database, std::chrono::milliseconds(1000)); 
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
