#include "redisjson++/transaction_manager.h"
#include "redisjson++/lua_script_manager.h" // For potential EVALSHA in transactions
#include <algorithm> // For std::transform with back_inserter if needed

namespace redisjson {

// --- TransactionManager::Transaction Implementation ---

TransactionManager::Transaction::Transaction(RedisConnectionManager::RedisConnectionPtr conn,
                                             PathParser* path_parser,
                                             JSONModifier* json_modifier)
    : connection_(std::move(conn)),
      path_parser_(path_parser),
      json_modifier_(json_modifier),
      active_(false),
      discarded_(false) {
    if (!connection_ || !connection_->is_connected()) {
        throw ConnectionException("Transaction created with invalid or disconnected connection.");
    }
    // PathParser and JSONModifier can be null if not used by this transaction's queued commands
}

TransactionManager::Transaction::~Transaction() {
    // If the transaction was started (MULTI sent) but not EXECuted or DISCARDed,
    // it's good practice to DISCARD it to clean up state on the server.
    // However, the connection itself will be closed if its unique_ptr is destroyed.
    // A DISCARD here might fail if connection is already broken.
    if (active_ && !discarded_ && connection_ && connection_->is_connected()) {
        try {
            discard(); // Attempt to clean up server state
        } catch (const std::exception& e) {
            // Log or handle discard failure during destruction. Can't throw from destructor easily.
            // std::cerr << "Error discarding transaction in destructor: " << e.what() << std::endl;
        }
    }
    // The connection_ unique_ptr will be destroyed, returning the connection to the manager
    // if RedisConnectionManager::return_connection was set up as a custom deleter for it,
    // OR if the manager's get_connection has specific return semantics.
    // Given current RedisConnectionManager simplified get/return, this unique_ptr just deletes the RedisConnection.
    // This needs to be aligned with how RedisConnectionManager actually lends/gives connections.
    // For now, assume unique_ptr dtor handles it appropriately based on manager's setup.
    // If using the simplified RedisConnectionManager, where get_connection creates new,
    // then this dtor correctly closes and frees this specific connection.
}


// Helper to convert various Redis reply types to JSON for transaction results
json TransactionManager::Transaction::redis_reply_to_json_transaction(redisReply* reply) const {
    if (!reply) return json(nullptr);

    switch (reply->type) {
        case REDIS_REPLY_STRING:
            // For transactions, a string reply is usually a direct value, not necessarily a JSON string to be parsed.
            // E.g. GET command returns string value. HGET returns field value.
            // If a command like JSON.GET (via Lua) returns a JSON string, then parse it.
            // This function needs to be context-aware or make assumptions.
            // For now, assume string replies are literal strings.
            // If a specific command in a transaction IS expected to return a JSON string (e.g. from a Lua script):
            // json_value = json::parse(reply->str, reply->str + reply->len);
            return std::string(reply->str, reply->len);
        case REDIS_REPLY_STATUS:
            return std::string(reply->str, reply->len); // e.g., "OK", "QUEUED"
        case REDIS_REPLY_INTEGER:
            return reply->integer;
        case REDIS_REPLY_NIL:
            return json(nullptr);
        case REDIS_REPLY_ERROR: {
            // This is an error from one of the commands *within* the transaction, after EXEC.
            // Or an error from MULTI, WATCH, DISCARD itself.
            // EXEC itself doesn't fail for command errors; it returns them in the array.
            // We should probably wrap this in a way that execute() can report partial success / individual errors.
            // For now, making it part of the json result.
            json error_obj;
            error_obj["error"] = true;
            error_obj["message"] = std::string(reply->str, reply->len);
            return error_obj;
        }
        case REDIS_REPLY_ARRAY: {
            // This occurs if EXEC returns array of replies, or a command like LRANGE.
            json::array_t arr;
            for (size_t i = 0; i < reply->elements; ++i) {
                arr.push_back(redis_reply_to_json_transaction(reply->element[i]));
            }
            return arr;
        }
        default:
            json err_obj;
            err_obj["error"] = true;
            err_obj["message"] = "Unknown or unsupported Redis reply type: " + std::to_string(reply->type);
            return err_obj;
    }
}


void TransactionManager::Transaction::queue_command(const std::string& cmd_name, std::vector<std::string> cmd_args) {
    if (discarded_) {
        throw TransactionException("Transaction has been discarded.");
    }
    if (!active_) { // Send MULTI first if not already active
        redisReply* multi_reply = connection_->command("MULTI");
        if (!multi_reply || strcmp(multi_reply->str, "OK") != 0) {
            if (multi_reply) freeReplyObject(multi_reply);
            throw TransactionException("Failed to start transaction (MULTI). Error: " +
                                       (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown"));
        }
        freeReplyObject(multi_reply);
        active_ = true;
    }

    // Build arguments for hiredis: command name + vector of string args
    std::vector<const char*> argv_c;
    std::vector<size_t> argv_len;

    argv_c.push_back(cmd_name.c_str());
    argv_len.push_back(cmd_name.length());

    for(const auto& arg : cmd_args) {
        argv_c.push_back(arg.c_str());
        argv_len.push_back(arg.length());
    }

    redisReply* queue_reply = connection_->command_argv(argv_c.size(), argv_c.data(), argv_len.data());

    if (!queue_reply || queue_reply->type == REDIS_REPLY_ERROR ||
        (queue_reply->type == REDIS_REPLY_STATUS && strcmp(queue_reply->str, "QUEUED") != 0)) {
        std::string err_details = "Failed to queue command '" + cmd_name + "'.";
        if (queue_reply) {
            if (queue_reply->type == REDIS_REPLY_ERROR) {
                err_details += " Error: " + std::string(queue_reply->str);
            } else {
                err_details += " Unexpected reply: " + std::string(queue_reply->str);
            }
            freeReplyObject(queue_reply);
        } else {
            err_details += " No reply from Redis (connection error: " + (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown") + ")";
        }
        // Attempt to DISCARD to clean up the transaction on Redis server side
        try {
            discard();
        } catch (const std::exception& e) {
            err_details += ". Additionally, failed to discard transaction: " + std::string(e.what());
        }
        throw TransactionException(err_details);
    }
    freeReplyObject(queue_reply);
    // Command successfully queued. Can store it locally if needed for later inspection, but not strictly necessary.
}

TransactionManager::Transaction& TransactionManager::Transaction::set_json_string(const std::string& key, const std::string& json_string_value) {
    queue_command("SET", {key, json_string_value});
    return *this;
}

TransactionManager::Transaction& TransactionManager::Transaction::get_json_string(const std::string& key) {
    queue_command("GET", {key});
    return *this;
}

TransactionManager::Transaction& TransactionManager::Transaction::del_json_document(const std::string& key) {
    queue_command("DEL", {key});
    return *this;
}

TransactionManager::Transaction& TransactionManager::Transaction::watch(const std::string& key) {
    if (active_) {
        throw TransactionException("WATCH command must be issued before MULTI.");
    }
    if (discarded_) {
        throw TransactionException("Transaction has been discarded.");
    }
    redisReply* watch_reply = connection_->command("WATCH %s", key.c_str());
    if (!watch_reply || strcmp(watch_reply->str, "OK") != 0) {
         if (watch_reply) freeReplyObject(watch_reply);
        throw TransactionException("Failed to WATCH key '" + key + "'. Error: " +
                                   (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown"));
    }
    freeReplyObject(watch_reply);
    return *this;
}

TransactionManager::Transaction& TransactionManager::Transaction::watch(const std::vector<std::string>& keys) {
    if (active_) {
        throw TransactionException("WATCH command must be issued before MULTI.");
    }
     if (discarded_) {
        throw TransactionException("Transaction has been discarded.");
    }
    if (keys.empty()) return *this;

    std::vector<const char*> argv_c;
    std::vector<size_t> argv_len;
    argv_c.push_back("WATCH");
    argv_len.push_back(strlen("WATCH"));
    for(const auto& key : keys) {
        argv_c.push_back(key.c_str());
        argv_len.push_back(key.length());
    }
    redisReply* watch_reply = connection_->command_argv(argv_c.size(), argv_c.data(), argv_len.data());
    if (!watch_reply || strcmp(watch_reply->str, "OK") != 0) {
        if (watch_reply) freeReplyObject(watch_reply);
        throw TransactionException("Failed to WATCH multiple keys. Error: " +
                                   (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown"));
    }
    freeReplyObject(watch_reply);
    return *this;
}


std::vector<json> TransactionManager::Transaction::execute() {
    if (discarded_) {
        throw TransactionException("Transaction has been discarded. Cannot execute.");
    }
    if (!active_) { // No commands were queued (MULTI not sent) or already executed/discarded
        // Or, if user calls execute() without queueing any commands, what should happen?
        // Redis EXEC without MULTI is an error. If MULTI wasn't called (no commands queued),
        // then active_ is false. Calling EXEC now would be a protocol error.
        // If no commands were queued, perhaps return empty vector or throw.
        // For now, assume if active_ is false, it means either no commands or already finished.
        // If it was already finished (EXEC/DISCARD called), active_ would be false.
        // Let's make it an error to call execute() more than once or on an empty transaction (no MULTI).
        if (command_queue_.empty() && !active_){ // Check active_ to see if MULTI was sent
             // This means no commands were added, so MULTI was not sent.
             // Or, if it was, it would be active.
             // For now, let's say it's okay to execute an "empty" transaction (just MULTI/EXEC)
             // But our queue_command sends MULTI on first command. So if queue is empty, MULTI wasn't sent.
             // The state `active_` tracks if MULTI was sent.
             // If `!active_` it means no commands were queued, or it was already EXEC/DISCARDED.
             // To prevent re-execution, we can add a `executed_` flag. For now, `active_` helps.
            throw TransactionException("Cannot execute: transaction is not active (no commands queued or already finalized).");
        }
    }

    redisReply* exec_reply = connection_->command("EXEC");
    active_ = false; // Transaction is no longer active after EXEC attempt
    // Note: WATCHed keys are automatically UNWATCHed by EXEC, regardless of success/failure.

    if (!exec_reply) { // Serious connection error or similar
        throw TransactionException("Failed to execute transaction (EXEC): No reply from Redis. Connection error: " +
                                   (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown"));
    }

    // Check for NIL reply from EXEC: means transaction was aborted (e.g., due to WATCH)
    if (exec_reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(exec_reply);
        discarded_ = true; // Explicitly mark as unusable, though active_ = false also indicates finality.
        throw TransactionException("Transaction aborted (e.g., optimistic lock failure). EXEC returned NIL.");
    }

    // Check for general EXEC error (should be rare, usually errors are in the array or NIL for abort)
    if (exec_reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = std::string(exec_reply->str, exec_reply->len);
        freeReplyObject(exec_reply);
        discarded_ = true;
        throw TransactionException("Failed to execute transaction (EXEC): " + err_msg);
    }

    if (exec_reply->type != REDIS_REPLY_ARRAY) {
        freeReplyObject(exec_reply);
        discarded_ = true; // Mark as unusable due to unexpected state
        throw TransactionException("Unexpected reply type from EXEC: expected ARRAY, got " + std::to_string(exec_reply->type));
    }

    std::vector<json> results;
    results.reserve(exec_reply->elements);
    for (size_t i = 0; i < exec_reply->elements; ++i) {
        results.push_back(redis_reply_to_json_transaction(exec_reply->element[i]));
    }

    freeReplyObject(exec_reply);
    // discarded_ = true; // Or some other flag like `executed_ = true;` to prevent reuse.
    // For now, `active_ = false` handles this.
    return results;
}

void TransactionManager::Transaction::discard() {
    if (discarded_) {
        return; // Already discarded
    }
    discarded_ = true; // Mark immediately to prevent further operations

    if (active_ && connection_ && connection_->is_connected()) {
        // If MULTI was sent (active_ is true), then DISCARD needs to be sent.
        // Otherwise, if only WATCH commands were sent, DISCARD is not strictly needed
        // as EXEC/QUIT/connection close will clear watches. But UNWATCH is cleaner if only watches.
        // For simplicity, if active_ (MULTI sent), send DISCARD.
        redisReply* discard_reply = connection_->command("DISCARD");
        active_ = false; // No longer active after DISCARD
        if (!discard_reply || discard_reply->type == REDIS_REPLY_ERROR) {
            std::string err_msg = "Failed to DISCARD transaction.";
            if (discard_reply && discard_reply->type == REDIS_REPLY_ERROR) {
                err_msg += " Error: " + std::string(discard_reply->str);
            } else if (!discard_reply) {
                 err_msg += " No reply (connection error: " + (connection_->get_context() ? std::string(connection_->get_context()->errstr) : "unknown") + ")";
            }
            if (discard_reply) freeReplyObject(discard_reply);
            throw TransactionException(err_msg); // Throw if DISCARD fails critically
        }
        freeReplyObject(discard_reply);
    } else if (!active_ && connection_ && connection_->is_connected()) {
        // If not `active_` (MULTI wasn't sent), but WATCH might have been. Send UNWATCH.
        // This is good practice if only WATCH was called.
        // However, EXEC/DISCARD/connection close also clears WATCHes.
        // For simplicity, current model ensures `active_` is true if any command (incl. potentially WATCH if it started MULTI) was queued.
        // If WATCH is always before MULTI (as it should be), then `active_` handles it.
        // If WATCH doesn't make it `active_`, then an explicit UNWATCH might be needed here.
        // The current `watch()` implementation does not set `active_`.
        // So, if only `watch()` was called, `active_` is false. We might want to UNWATCH.
        // Let's assume for now that connection closure or a subsequent (failed) EXEC handles UNWATCH.
        // Or, client explicitly calls UNWATCH if needed outside transaction scope.
        // For robust WATCH management, `Transaction` should track if it has pending WATCHes.
    }
    // `active_` should be false now.
    // `command_queue_` is cleared implicitly as the transaction object won't be used further for queuing.
}


// --- TransactionManager Implementation ---
TransactionManager::TransactionManager(RedisConnectionManager* conn_manager,
                                       PathParser* path_parser,
                                       JSONModifier* json_modifier)
    : connection_manager_(conn_manager),
      path_parser_(path_parser),
      json_modifier_(json_modifier) {
    if (!connection_manager_) {
        throw std::invalid_argument("RedisConnectionManager cannot be null for TransactionManager.");
    }
    // path_parser and json_modifier can be null if Transaction implementation doesn't need them directly.
}

TransactionManager::~TransactionManager() {}

std::unique_ptr<TransactionManager::Transaction> TransactionManager::begin_transaction() {
    auto conn = connection_manager_->get_connection(); // This will throw if simplified get_connection fails
    if (!conn || !conn->is_connected()) {
        // get_connection should throw if it can't provide a valid connection.
        // This is a redundant check if get_connection is robust.
        throw ConnectionException("Failed to obtain a valid Redis connection for transaction from manager.");
    }
    // Pass dependencies to Transaction constructor.
    // Transaction takes ownership of the connection unique_ptr.
    return std::make_unique<Transaction>(std::move(conn), path_parser_, json_modifier_);
}

// void TransactionManager::enable_optimistic_locking(bool enabled) {
//     optimistic_locking_default_ = enabled;
// }

} // namespace redisjson
