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
    // LOGGING: Entering connect()
    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - Entry." << std::endl;
    if (connected_) {
        std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - Already connected." << std::endl;
        return true;
    }
    last_error_message_.clear();

    struct timeval tv_timeout;
    tv_timeout.tv_sec = connect_timeout_ms_.count() / 1000;
    tv_timeout.tv_usec = (connect_timeout_ms_.count() % 1000) * 1000;
    // LOGGING: Before redisConnectWithTimeout
    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_
              << " - Calling redisConnectWithTimeout (timeout: " << tv_timeout.tv_sec << "s "
              << tv_timeout.tv_usec << "us)." << std::endl;

    context_ = redisConnectWithTimeout(host_.c_str(), port_, tv_timeout);

    // LOGGING: After redisConnectWithTimeout
    if (context_ == nullptr || context_->err) {
        std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_
                  << " - redisConnectWithTimeout failed. Context is " << (context_ ? "not null" : "null")
                  << (context_ && context_->err ? ", context error: " + std::string(context_->errstr ? context_->errstr : "Unknown error") + " (code: " + std::to_string(context_->err) + ")" : "")
                  << std::endl;
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
    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - redisConnectWithTimeout succeeded." << std::endl;

    // LOGGING: Before redisSetTimeout
    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_
              << " - Calling redisSetTimeout (timeout: " << tv_timeout.tv_sec << "s "
              << tv_timeout.tv_usec << "us)." << std::endl;
    if (redisSetTimeout(context_, tv_timeout) != REDIS_OK) {
        std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_
                  << " - redisSetTimeout failed: "
                  << (context_->errstr ? std::string(context_->errstr) : "Unknown error")
                  << " (code: " << std::to_string(context_->err) << ")" << std::endl;
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
    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - redisSetTimeout succeeded." << std::endl;

    if (!authenticate()) {
       if (last_error_message_.empty()) last_error_message_ = "Authentication failed";
        // LOGGING: Authentication failed
        std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - Authentication failed. Error: " << last_error_message_ << std::endl;
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }
    // LOGGING: Authentication succeeded (or not needed) is logged within authenticate()

    if (!select_database()) {
       if (last_error_message_.empty()) last_error_message_ = "Database selection failed";
        // LOGGING: Database selection failed
        std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - Database selection failed. Error: " << last_error_message_ << std::endl;
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }
    // LOGGING: Database selection succeeded (or not needed) is logged within select_database()

    std::cout << "LOG: RedisConnection::connect() for " << host_ << ":" << port_ << " - Connection fully established." << std::endl;
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
    // LOGGING: Entering authenticate()
    // std::cout << "LOG: RedisConnection::authenticate() for " << host_ << ":" << port_ << " - Entry." << std::endl;
    if (password_.empty()) {
        std::cout << "LOG: RedisConnection::authenticate() for " << host_ << ":" << port_ << " - No password provided, skipping AUTH." << std::endl;
        return true;
    }
    std::cout << "LOG: RedisConnection::authenticate() for " << host_ << ":" << port_ << " - Calling redisCommand for AUTH." << std::endl;
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "AUTH %s", password_.c_str()));

    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        std::cout << "LOG: RedisConnection::authenticate() for " << host_ << ":" << port_
                  << " - AUTH failed. Reply is " << (reply ? "not null" : "null")
                  << (reply && reply->type == REDIS_REPLY_ERROR ? ", reply error: " + std::string(reply->str ? reply->str : "Unknown error") : "")
                  << (!reply && context_->err ? ", context error: " + std::string(context_->errstr ? context_->errstr : "Unknown error") + " (code: " + std::to_string(context_->err) + ")" : "")
                  << std::endl;
       if (reply && reply->str) {
           last_error_message_ = "Authentication failed: " + std::string(reply->str);
       } else if (context_->errstr && context_->err != 0) { // Check context error if reply is null
           last_error_message_ = "Authentication failed: " + std::string(context_->errstr) + " (code: " + std::to_string(context_->err) + ")";
       } else {
           last_error_message_ = "Authentication failed: No reply or unknown error from AUTH command.";
       }
        if (reply) freeReplyObject(reply);
        return false;
    }
    std::cout << "LOG: RedisConnection::authenticate() for " << host_ << ":" << port_ << " - AUTH succeeded." << std::endl;
    freeReplyObject(reply);
    return true;
}

bool RedisConnection::select_database() {
    // LOGGING: Entering select_database()
    // std::cout << "LOG: RedisConnection::select_database() for " << host_ << ":" << port_ << " - Entry. DB: " << database_ << std::endl;
    if (database_ == 0) {
        std::cout << "LOG: RedisConnection::select_database() for " << host_ << ":" << port_ << " - Database is 0, skipping SELECT." << std::endl;
        return true;
    }
    std::cout << "LOG: RedisConnection::select_database() for " << host_ << ":" << port_ << " - Calling redisCommand for SELECT " << database_ << "." << std::endl;
    redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "SELECT %d", database_));

    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        std::cout << "LOG: RedisConnection::select_database() for " << host_ << ":" << port_
                  << " - SELECT " << database_ << " failed. Reply is " << (reply ? "not null" : "null")
                  << (reply && reply->type == REDIS_REPLY_ERROR ? ", reply error: " + std::string(reply->str ? reply->str : "Unknown error") : "")
                  << (!reply && context_->err ? ", context error: " + std::string(context_->errstr ? context_->errstr : "Unknown error") + " (code: " + std::to_string(context_->err) + ")" : "")
                  << std::endl;
       if (reply && reply->str) {
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: " + std::string(reply->str);
       } else if (context_->errstr && context_->err != 0) { // Check context error if reply is null
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: " + std::string(context_->errstr) + " (code: " + std::to_string(context_->err) + ")";
       } else {
           last_error_message_ = "SELECT database " + std::to_string(database_) + " failed: No reply or unknown error from SELECT command.";
       }
        if (reply) freeReplyObject(reply);
        return false;
    }
    std::cout << "LOG: RedisConnection::select_database() for " << host_ << ":" << port_ << " - SELECT " << database_ << " succeeded." << std::endl;
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
    std::cout << "LOG: RedisConnectionManager::initialize_pool() - Entry. Pool size: " << config_.connection_pool_size << std::endl;
    std::lock_guard<std::mutex> lock(pool_mutex_);
    for (int i = 0; i < config_.connection_pool_size; ++i) {
        std::cout << "LOG: RedisConnectionManager::initialize_pool() - Attempting to create initial connection " << (i + 1) << "/" << config_.connection_pool_size << std::endl;
        RedisConnection* conn_raw = new RedisConnection(config_.host, config_.port, config_.password, config_.database, config_.timeout);
        bool connected_successfully = false;

        if (conn_raw) {
            std::cout << "LOG: RedisConnectionManager::initialize_pool() - Connection " << (i+1) << " allocated. Calling connect()." << std::endl;
            connected_successfully = conn_raw->connect();
        } else {
            std::cout << "LOG: RedisConnectionManager::initialize_pool() - Connection " << (i+1) << " FAILED TO ALLOCATE (new returned null)." << std::endl;
        }

        if (connected_successfully) {
            std::cout << "LOG: RedisConnectionManager::initialize_pool() - Connection " << (i+1) << " connected successfully." << std::endl;
            RedisConnectionManager::RedisConnectionPtr conn(conn_raw, RedisConnectionDeleter(this));
            available_connections_.push(conn.get());
            pool_.push_back(std::move(conn));
            stats_.idle_connections++;
            stats_.total_connections++;
            if (pool_.size() == 1 && i == 0) primary_healthy_ = true;
        } else {
            std::cout << "LOG: RedisConnectionManager::initialize_pool() - Connection " << (i+1) << " FAILED to connect. Last error: " << (conn_raw ? conn_raw->get_last_error() : "N/A (allocation failed)") << std::endl;
            stats_.connection_errors++;
            if (conn_raw) {
                delete conn_raw;
            }
            if (i == 0) primary_healthy_ = false;
        }
    }
    std::cout << "LOG: RedisConnectionManager::initialize_pool() - Exit. Total connections in pool: " << pool_.size()
              << ", Available: " << available_connections_.size()
              << ", Stats(total/active/idle/errors): "
              << stats_.total_connections.load() << "/" << stats_.active_connections.load() << "/"
              << stats_.idle_connections.load() << "/" << stats_.connection_errors.load() << std::endl;
}

RedisConnectionManager::RedisConnectionPtr RedisConnectionManager::get_connection() {
    std::cout << "LOG: RedisConnectionManager::get_connection() - Entry. Thread ID: " << std::this_thread::get_id() << std::endl;
    std::unique_lock<std::mutex> lock(pool_mutex_);
    std::cout << "LOG: RedisConnectionManager::get_connection() - Acquired pool_mutex_. Thread ID: " << std::this_thread::get_id() << std::endl;


    while (true) {
        if (shutting_down_) {
            std::cout << "LOG: RedisConnectionManager::get_connection() - Shutting down. Throwing ConnectionException. Thread ID: " << std::this_thread::get_id() << std::endl;
            throw ConnectionException("Connection manager is shutting down.");
        }

        std::cout << "LOG: RedisConnectionManager::get_connection() - Before condition_variable.wait. Predicate: (shutting_down_ || !available_connections_.empty() || ((stats_.active_connections + pool_.size()) < config_.connection_pool_size)). Values: available_empty="
                  << available_connections_.empty() << ", active=" << stats_.active_connections.load()
                  << ", pool_v_sz=" << pool_.size() << ", config_pool_sz=" << config_.connection_pool_size
                  << ". Thread ID: " << std::this_thread::get_id() << std::endl;

        condition_.wait(lock, [this] {
            // This predicate is logged before the wait by the caller
            return shutting_down_ ||
                   !available_connections_.empty() ||
                   ((stats_.active_connections + pool_.size()) < static_cast<size_t>(this->config_.connection_pool_size));
        });

        std::cout << "LOG: RedisConnectionManager::get_connection() - Woke up from condition_variable.wait. Thread ID: " << std::this_thread::get_id() << std::endl;

        if (shutting_down_) {
            std::cout << "LOG: RedisConnectionManager::get_connection() - Shutting down (checked after wait). Throwing ConnectionException. Thread ID: " << std::this_thread::get_id() << std::endl;
            throw ConnectionException("Connection manager is shutting down while waiting for a connection.");
        }

        RedisConnectionManager::RedisConnectionPtr conn_to_return;

        if (!available_connections_.empty()) {
            std::cout << "LOG: RedisConnectionManager::get_connection() - Path: Taking from available_connections_ (size=" << available_connections_.size() << "). Thread ID: " << std::this_thread::get_id() << std::endl;
            RedisConnection* raw_conn = available_connections_.front();
            available_connections_.pop();
            stats_.idle_connections--;

            auto it = std::find_if(pool_.begin(), pool_.end(),
                                   [&](const RedisConnectionManager::RedisConnectionPtr& p) { return p.get() == raw_conn; });

            if (it != pool_.end()) {
                conn_to_return = std::move(*it);
                pool_.erase(it);
                std::cout << "LOG: RedisConnectionManager::get_connection() - Moved connection from pool_ vector. New pool_ vector size: " << pool_.size() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
            } else {
                std::cout << "ERROR_LOG: RedisConnectionManager::get_connection() - Connection from available_connections_ not found in pool_ vector! This should not happen. Retrying. Thread ID: " << std::this_thread::get_id() << std::endl;
                stats_.idle_connections++;
                continue;
            }

            bool healthy = true;
            if (conn_to_return && conn_to_return->is_connected()) {
                if (std::chrono::steady_clock::now() - conn_to_return->last_used_time > max_idle_time_before_ping_) {
                    std::cout << "LOG: RedisConnectionManager::get_connection() - Idle connection requires ping. Unlocking for ping. Thread ID: " << std::this_thread::get_id() << std::endl;
                    lock.unlock();
                    std::cout << "LOG: RedisConnectionManager::get_connection() - Calling ping() for " << conn_to_return->get_host() << ":" << conn_to_return->get_port() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
                    healthy = conn_to_return->ping();
                    std::cout << "LOG: RedisConnectionManager::get_connection() - Ping result: " << healthy << ". Relocking. Thread ID: " << std::this_thread::get_id() << std::endl;
                    lock.lock();

                    if (shutting_down_) {
                         std::cout << "LOG: RedisConnectionManager::get_connection() - Shutting down during health check. Disconnecting and throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
                        if (conn_to_return) {
                            conn_to_return->disconnect();
                        }
                        throw ConnectionException("Connection manager is shutting down during health check.");
                    }
                }
            } else if (conn_to_return) {
                healthy = false;
                std::cout << "LOG: RedisConnectionManager::get_connection() - Connection from pool was not connected. Healthy=false. Thread ID: " << std::this_thread::get_id() << std::endl;
            } else {
                healthy = false;
                std::cout << "ERROR_LOG: RedisConnectionManager::get_connection() - conn_to_return is null after move from pool. Healthy=false. This is unexpected. Thread ID: " << std::this_thread::get_id() << std::endl;
            }

            if (!healthy) {
                std::cout << "LOG: RedisConnectionManager::get_connection() - Connection not healthy. Discarding. Thread ID: " << std::this_thread::get_id() << std::endl;
                stats_.connection_errors++;
                if (conn_to_return) {
                    stats_.total_connections--;
                    std::cout << "LOG: RedisConnectionManager::get_connection() - Decremented total_connections to " << stats_.total_connections.load() << ". Releasing and deleting. Thread ID: " << std::this_thread::get_id() << std::endl;
                    RedisConnection* raw_to_delete = conn_to_return.release();
                    delete raw_to_delete;
                }
                continue;
            }
            stats_.active_connections++;
            std::cout << "LOG: RedisConnectionManager::get_connection() - Returning healthy connection from pool. Active: " << stats_.active_connections.load() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
            return conn_to_return;

        } else if ((stats_.active_connections + pool_.size()) < static_cast<size_t>(config_.connection_pool_size)) {
            std::cout << "LOG: RedisConnectionManager::get_connection() - Path: Creating new connection. Active=" << stats_.active_connections.load()
                      << ", pool_v_sz=" << pool_.size() << ", config_pool_sz=" << config_.connection_pool_size
                      << ". Unlocking for create. Thread ID: " << std::this_thread::get_id() << std::endl;
            lock.unlock();

            std::cout << "LOG: RedisConnectionManager::get_connection() - Calling create_new_connection_ptr(). Thread ID: " << std::this_thread::get_id() << std::endl;
            RedisConnectionManager::RedisConnectionPtr new_conn = create_new_connection_ptr(config_.host, config_.port);
            std::cout << "LOG: RedisConnectionManager::get_connection() - create_new_connection_ptr() returned. Relocking. Thread ID: " << std::this_thread::get_id() << std::endl;
            lock.lock();

            if (shutting_down_) {
                std::cout << "LOG: RedisConnectionManager::get_connection() - Shutting down during new connection creation. Disconnecting and throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
                if(new_conn) new_conn->disconnect(); // The unique_ptr deleter will handle return_connection
                throw ConnectionException("Connection manager is shutting down during new connection creation.");
            }

            if (new_conn && new_conn->is_connected()) {
                stats_.total_connections++;
                stats_.active_connections++;
                std::cout << "LOG: RedisConnectionManager::get_connection() - Returning newly created healthy connection. Total: "
                          << stats_.total_connections.load() << ", Active: " << stats_.active_connections.load()
                          << ". Thread ID: " << std::this_thread::get_id() << std::endl;
                return new_conn;
            } else {
                stats_.connection_errors++;
                std::string error_detail = (new_conn ? new_conn->get_last_error() : "Failed to allocate RedisConnection object.");
                std::cout << "ERROR_LOG: RedisConnectionManager::get_connection() - Failed to create or connect new connection. Error: " << error_detail << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
                // new_conn unique_ptr will be destroyed, its deleter calls return_connection, which will delete the raw object.
                throw ConnectionException("Failed to create new connection to " + config_.host + ":" + std::to_string(config_.port) + ". Detail: " + error_detail);
            }
        } else {
            // This should ideally not be reached if wait predicate is correct and config_.connection_pool_size > 0.
            // If it is, it means available_connections_ is empty AND (active + pool_.size()) >= config_.connection_pool_size
            std::cout << "ERROR_LOG: RedisConnectionManager::get_connection() - No available connections and pool is at max capacity. Throwing. This state should ideally be caught by CV wait. Active="
                      << stats_.active_connections.load() << ", pool_v_sz=" << pool_.size() << ", config_pool_sz=" << config_.connection_pool_size
                      << ". Thread ID: " << std::this_thread::get_id() << std::endl;
            throw ConnectionException("No available connections and pool is at max capacity and cannot create more.");
        }
    }
}

void RedisConnectionManager::return_connection(RedisConnectionManager::RedisConnectionPtr conn_param_owner) {
    std::cout << "LOG: RedisConnectionManager::return_connection() - Entry. Conn valid: " << (conn_param_owner ? "true" : "false")
              << (conn_param_owner ? ", Host: " + conn_param_owner->get_host() + ":" + std::to_string(conn_param_owner->get_port()) : "")
              << ". Thread ID: " << std::this_thread::get_id() << std::endl;

    if (!conn_param_owner) {
        std::cout << "LOG: RedisConnectionManager::return_connection() - Null connection pointer passed, returning early. Thread ID: " << std::this_thread::get_id() << std::endl;
        return;
    }

    RedisConnection* conn_raw = conn_param_owner.get();
    bool repool_this_connection = false;
    bool needs_notification = false;

    {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        std::cout << "LOG: RedisConnectionManager::return_connection() - Acquired pool_mutex_. Active before dec: " << stats_.active_connections.load() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
        stats_.active_connections--;
        std::cout << "LOG: RedisConnectionManager::return_connection() - Active after dec: " << stats_.active_connections.load() << ". Thread ID: " << std::this_thread::get_id() << std::endl;


        if (shutting_down_) {
            std::cout << "LOG: RedisConnectionManager::return_connection() - Shutting down. Disconnecting connection. Thread ID: " << std::this_thread::get_id() << std::endl;
            conn_raw->disconnect();
        } else if (conn_raw->is_connected() && pool_.size() < static_cast<size_t>(config_.connection_pool_size)) {
            std::cout << "LOG: RedisConnectionManager::return_connection() - Connection is connected and pool has space (pool_v_sz=" << pool_.size() << ", config_pool_sz=" << config_.connection_pool_size << "). Repooling. Thread ID: " << std::this_thread::get_id() << std::endl;
            repool_this_connection = true;
            available_connections_.push(conn_raw);
            pool_.push_back(std::move(conn_param_owner));
            stats_.idle_connections++;
            needs_notification = true;
        } else {
            std::cout << "LOG: RedisConnectionManager::return_connection() - Not repooling. Shutting down: " << shutting_down_.load()
                      << ", Connected: " << conn_raw->is_connected()
                      << ", Pool full? (pool_v_sz=" << pool_.size() << ", config_pool_sz=" << config_.connection_pool_size << ")"
                      << ". Disconnecting. Thread ID: " << std::this_thread::get_id() << std::endl;
            if (!conn_raw->is_connected() && !shutting_down_) {
                stats_.connection_errors++;
            }
            conn_raw->disconnect();
        }

        if (!repool_this_connection) {
            if (stats_.total_connections > 0) {
                 stats_.total_connections--;
                 needs_notification = true;
                 std::cout << "LOG: RedisConnectionManager::return_connection() - Connection not repooled, decremented total_connections to " << stats_.total_connections.load() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
            } else {
                 std::cout << "LOG: RedisConnectionManager::return_connection() - Connection not repooled, but total_connections already 0. Not decrementing. Thread ID: " << std::this_thread::get_id() << std::endl;
            }
        }

        if (needs_notification) {
            std::cout << "LOG: RedisConnectionManager::return_connection() - Calling condition_.notify_one(). Thread ID: " << std::this_thread::get_id() << std::endl;
            condition_.notify_one();
        }
        std::cout << "LOG: RedisConnectionManager::return_connection() - Releasing pool_mutex_. Thread ID: " << std::this_thread::get_id() << std::endl;
    }

    if (!repool_this_connection) {
        std::cout << "LOG: RedisConnectionManager::return_connection() - Connection was not repooled. Releasing and deleting raw pointer. Thread ID: " << std::this_thread::get_id() << std::endl;
        RedisConnection* to_delete = conn_param_owner.release();
        delete to_delete;
    } else {
        std::cout << "LOG: RedisConnectionManager::return_connection() - Connection was repooled. conn_param_owner is now null. Thread ID: " << std::this_thread::get_id() << std::endl;
    }
    std::cout << "LOG: RedisConnectionManager::return_connection() - Exit. Thread ID: " << std::this_thread::get_id() << std::endl;
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
                    // total_connections will be decremented by return_connection when the object is finally destroyed.
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
