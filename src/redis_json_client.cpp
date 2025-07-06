#include "redisjson++/redis_json_client.h"
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr
#include <stdexcept>
#include <string> // Required for std::to_string with some compilers/setups

// For PathParser, JSONModifier, LuaScriptManager - include their headers once they exist
// For now, we might not be able to fully initialize them if their constructors are complex.

namespace redisjson {

RedisJSONClient::RedisJSONClient(const ClientConfig& client_config)
    : _config(client_config) {
    _connection_manager = std::make_unique<RedisConnectionManager>(_config);

    // Initialize other managers - for now, assume default constructors or simple ones
    // Once these classes are defined, their proper initialization will occur here.
    _path_parser = std::make_unique<PathParser>();
    _json_modifier = std::make_unique<JSONModifier>();
    // LuaScriptManager constructor expects RedisConnectionManager*
    _lua_script_manager = std::make_unique<LuaScriptManager>(_connection_manager.get());
    if (_lua_script_manager) {
        try {
            _lua_script_manager->preload_builtin_scripts();
        } catch (const RedisException& e) {
            // Handle or log critical error during client initialization
            // For example, throw a specific client initialization error
            throw RedisConfigurationException("Failed to preload Lua scripts during RedisJSONClient construction: " + std::string(e.what()));
        }
    }
    // Query engine and other components that might depend on Lua scripts or other services
    // should be initialized after their dependencies.
    _query_engine = std::make_unique<JSONQueryEngine>(*this); // If it depends on RedisJSONClient itself
    _json_cache = std::make_unique<JSONCache>(); // Example: Default constructor
    _schema_validator = std::make_unique<JSONSchemaValidator>(); // Example: Default constructor
    _event_emitter = std::make_unique<JSONEventEmitter>(); // Example: Default constructor
    _transaction_manager = std::make_unique<TransactionManager>(_connection_manager.get(), _path_parser.get(),_json_modifier.get());
}

RedisJSONClient::~RedisJSONClient() {
    // Resources managed by unique_ptr will be cleaned up automatically.
    // If LuaScriptManager or others have explicit shutdown needs, call them here.
}

std::unique_ptr<RedisConnection> RedisJSONClient::get_redis_connection() const {
    try {
        return _connection_manager->get_connection();
    } catch (const ConnectionException& e) {
        // Log error or rethrow as a more specific client error if desired
        throw; // Rethrow for now
    }
}

// --- Document Operations ---

void RedisJSONClient::set_json(const std::string& key, const json& document, const SetOptions& opts) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    std::string doc_str = document.dump(); // Serialize JSON to string

    RedisReplyPtr reply;

    if (opts.ttl.count() > 0) {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("SET %s %s EX %lld", key.c_str(), doc_str.c_str(), (long long)opts.ttl.count())
        ));
    } else {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("SET %s %s", key.c_str(), doc_str.c_str())
        ));
    }

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn)); // Return connection before throwing
        throw RedisCommandException("SET", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }
    // Check reply status for SET, should be "OK"
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") != 0) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("SET", "Key: " + key + ", SET command did not return OK: " + std::string(reply->str));
    }

    _connection_manager->return_connection(std::move(conn));
}

json RedisJSONClient::get_json(const std::string& key) const {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("GET %s", key.c_str())
    ));

    if (!reply) { // Covers null reply from command() itself (e.g. connection broken before command sent)
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("GET", "Key: " + key + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("GET", "Key: " + key + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        // Using PathNotFoundException as KeyNotFoundException is not standard in exceptions.h
        throw PathNotFoundException(key, "$ (root)");
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string doc_str = reply->str;
        _connection_manager->return_connection(std::move(conn));
        try {
            return json::parse(doc_str);
        } catch (const json::parse_error& e) {
            // Adapt to existing JsonParsingException constructor
            throw JsonParsingException("Failed to parse JSON string for key '" + key + "': " + e.what() + ". Received: " + doc_str);
        }
    }

    // Should not reach here with a valid Redis GET reply structure
    _connection_manager->return_connection(std::move(conn));
    // This path indicates a logic error or unexpected server behavior.
    // Return a default-constructed json or throw a more specific internal error.
    // For now, let's throw, consistent with other error handling.
    throw RedisCommandException("GET", "Key: " + key + ", Error: Unexpected reply type from Redis: " + std::to_string(reply ? reply->type : -1));
}

bool RedisJSONClient::exists_json(const std::string& key) const {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("EXISTS %s", key.c_str())
    ));

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("EXISTS", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }

    _connection_manager->return_connection(std::move(conn));
    return (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
}

void RedisJSONClient::del_json(const std::string& key) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("DEL %s", key.c_str())
    ));

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("DEL", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }
    // DEL returns number of keys deleted. We don't strictly need to check it for success
    // unless we want to confirm it was > 0 for an "effective" delete.
    // For now, no error from Redis is success.
    _connection_manager->return_connection(std::move(conn));
}

// --- Path Operations ---

json RedisJSONClient::get_path(const std::string& key, const std::string& path) const {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.GET %s %s", key.c_str(), path.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.GET", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.GET", "Key: " + key + ", Path: " + path + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        throw PathNotFoundException(key, path);
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string result_str = reply->str;
        _connection_manager->return_connection(std::move(conn));
        try {
            // JSON.GET for a path can return an array of values if the path is a multi-path expression,
            // or a single JSON value if the path points to a single value.
            // The result is always a string that needs to be parsed as JSON.
            // If it's a single value (e.g. a number like `42` or a string like `"hello"`), 
            // nlohmann::json::parse will handle it correctly.
            // If it's an array of values (e.g. `[1,2,3]`), it will also be parsed correctly.
            return _parse_json_reply(result_str, "JSON.GET for key '" + key + "', path '" + path + "'");
        } catch (const JsonParsingException& e) {
            // Rethrow if it's already a JsonParsingException from _parse_json_reply
            throw;
        } catch (const json::parse_error& e) {
            // Catch other potential parse errors from nlohmann::json directly
            throw JsonParsingException("Failed to parse JSON.GET response for key '" + key + "', path '" + path + "': " + e.what() + ". Received: " + result_str);
        }
    }

    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.GET", "Key: " + key + ", Path: " + path + ", Error: Unexpected reply type from Redis: " + std::to_string(reply->type));
}

void RedisJSONClient::set_path(const std::string& key, const std::string& path,
                               const json& value, const SetOptions& opts) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    std::string value_str = value.dump();
    RedisReplyPtr reply;

    std::string command_str = "JSON.SET " + key + " " + path + " '" + value_str + "'"; // Basic command
    // Note: Directly embedding value_str can be problematic if it contains spaces or special characters
    // not handled by hiredis variadic command formatting.
    // It's safer to pass it as a separate argument to conn->command.

    if (opts.condition == SetCondition::NX) {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("JSON.SET %s %s %s NX", key.c_str(), path.c_str(), value_str.c_str())
        ));
    } else if (opts.condition == SetCondition::XX) {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("JSON.SET %s %s %s XX", key.c_str(), path.c_str(), value_str.c_str())
        ));
    } else { // Condition::NONE or default
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("JSON.SET %s %s %s", key.c_str(), path.c_str(), value_str.c_str())
        ));
    }

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.SET", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.SET", "Key: " + key + ", Path: " + path + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.SET returns "OK" on success.
    // If NX or XX is used, it returns NULL if the condition is not met.
    if (reply->type == REDIS_REPLY_NIL) {
        // This means condition (NX or XX) was not met. This is not an "error" but a failed conditional set.
        // The current function signature is void, so we can't directly return false.
        // We could throw a specific exception like ConditionNotMetException, or just return.
        // For now, let's assume that if a condition is specified and not met, it's not an error
        // that should halt execution by throwing, but the operation didn't proceed.
        // If the SetOptions also included TTL, that part would be ignored by Redis.
        _connection_manager->return_connection(std::move(conn));
        // If we need to signal this specific outcome, the function signature or an output parameter would be needed.
        // For now, just returning normally implies the command was sent and Redis replied without error.
        return;
    }

    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        // Success
        // Now, handle TTL if specified. JSON.SET itself doesn't take TTL.
        // We need to use a separate EXPIRE command if opts.ttl is set.
        // This is not atomic with the JSON.SET. For atomicity, a Lua script would be needed.
        if (opts.ttl.count() > 0) {
            RedisReplyPtr expire_reply(static_cast<redisReply*>(
                conn->command("EXPIRE %s %lld", key.c_str(), (long long)opts.ttl.count())
            ));
            if (!expire_reply || expire_reply->type == REDIS_REPLY_ERROR) {
                // Log this error? Or throw? If EXPIRE fails, the SET was still successful.
                // For now, let's throw as it's an unexpected error in a dependent command.
                _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("EXPIRE", "Failed to set TTL for key " + key + " after JSON.SET. Error: " + (expire_reply && expire_reply->str ? expire_reply->str : "Unknown error"));
            }
        }
        _connection_manager->return_connection(std::move(conn));
        return;
    }

    // Unexpected reply
    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.SET", "Key: " + key + ", Path: " + path + ", Error: Unexpected reply: " + (reply->str ? reply->str : "Unknown reply type/status"));
}

void RedisJSONClient::del_path(const std::string& key, const std::string& path) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.DEL %s %s", key.c_str(), path.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.DEL", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.DEL", "Key: " + key + ", Path: " + path + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.DEL returns an integer: the number of paths deleted (0 or 1 in this case, as we provide a single path).
    // If the key or path does not exist, it returns 0. This is not an error.
    if (reply->type == REDIS_REPLY_INTEGER) {
        // int paths_deleted = reply->integer;
        // We don't need to do anything with paths_deleted for a void function,
        // unless we want to throw if it's 0, but that's usually not the behavior for DEL.
        _connection_manager->return_connection(std::move(conn));
        return;
    }

    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.DEL", "Key: " + key + ", Path: " + path + ", Error: Unexpected reply type from Redis: " + std::to_string(reply->type));
}

bool RedisJSONClient::exists_path(const std::string& key, const std::string& path) const {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.TYPE %s %s", key.c_str(), path.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        // Distinguish between no reply and actual "not found"
        throw RedisCommandException("JSON.TYPE", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        // Some errors might indicate a non-existent key, but JSON.TYPE should return NIL for that.
        // So, any error here is likely a syntax issue or server problem.
        std::string error_msg = (reply->str ? reply->str : "Redis error reply with no message");
        // Check if the error message indicates the key itself does not exist, which some Redis versions might do for JSON commands
        // e.g. "ERR new objects must be created at the root" - this is not for TYPE but as an example of error string.
        // For JSON.TYPE, if the key does not exist, it should return REDIS_REPLY_NIL for the path.
        // If the path does not exist under an existing key, it also returns REDIS_REPLY_NIL.
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.TYPE", "Key: " + key + ", Path: " + path + ", Error: " + error_msg);
    }

    // JSON.TYPE returns NIL if the key or path does not exist.
    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        return false; // Key or path does not exist
    }

    // If it's not NIL and not an ERROR, it means the path exists and has a type.
    // The reply will be a string representing the type (e.g., "object", "array", "string").
    // We don't need to check the specific type, just that we got a type string.
    if (reply->type == REDIS_REPLY_STRING) {
        _connection_manager->return_connection(std::move(conn));
        return true; // Path exists
    }
    
    // Any other reply type is unexpected for JSON.TYPE
    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.TYPE", "Key: " + key + ", Path: " + path + ", Error: Unexpected reply type from Redis: " + std::to_string(reply->type));
}

// --- Array Operations ---

void RedisJSONClient::append_path(const std::string& key, const std::string& path,
                                  const json& value) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    std::string value_str = value.dump();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.ARRAPPEND %s %s %s", key.c_str(), path.c_str(), value_str.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRAPPEND", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRAPPEND", "Key: " + key + ", Path: " + path + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.ARRAPPEND returns an array of integers, where each integer is the new length of the array after appending a value.
    // If a single path is specified and a single value is appended, it returns an array with one integer (the new length),
    // or NIL if the path is not an array or the key does not exist.
    // Example: `JSON.ARRAPPEND mykey .arr 1 2` returns `[3,4]` if .arr was `[0]`
    // If just `JSON.ARRAPPEND mykey .arr 1`, returns `[2]` if .arr was `[0]`
    // If path is not an array, or key does not exist, returns `[nil]` (array of nils in hiredis?) or just `NIL`
    // The command `JSON.ARRAPPEND <key> <path> <json> [json ...]`
    // Hiredis reply for `[integer]` would be an array reply with one element of type integer.
    // Hiredis reply for `[nil]` would be an array reply with one element of type nil.
    // Hiredis reply for `NIL` (e.g. key not found) would be a REDIS_REPLY_NIL.

    if (reply->type == REDIS_REPLY_NIL) {
        // This means the key does not exist, or the path before the last segment does not exist.
        _connection_manager->return_connection(std::move(conn));
        // This could be an error or interpreted as "operation had no effect".
        // Throwing an exception seems appropriate if the expectation is that the path should exist.
        throw PathNotFoundException(key, path, "Key or path does not exist, or path is not an array, for JSON.ARRAPPEND");
    }
    
    if (reply->type == REDIS_REPLY_ARRAY) {
        // Expecting an array with one element, which is an integer (new length) or nil (path is not an array type).
        if (reply->elements == 1) {
            redisReply* element_reply = reply->element[0];
            if (element_reply->type == REDIS_REPLY_INTEGER) {
                // Success, new length is element_reply->integer. We don't do anything with it for a void function.
                _connection_manager->return_connection(std::move(conn));
                return;
            } else if (element_reply->type == REDIS_REPLY_NIL) {
                // This means the path exists but is not an array.
                _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("JSON.ARRAPPEND", "Key: " + key + ", Path: " + path + " exists but is not an array.");
            }
        }
    }

    _connection_manager->return_connection(std::move(conn));
    // Fallback for unexpected reply structures.
    // Construct a more informative message if possible.
    std::string reply_details = "Unexpected reply type: " + std::to_string(reply->type);
    if (reply->type == REDIS_REPLY_STRING) {
        reply_details += ", Value: " + std::string(reply->str);
    } else if (reply->type == REDIS_REPLY_ARRAY) {
        reply_details += ", Elements: " + std::to_string(reply->elements);
         if (reply->elements > 0 && reply->element[0]) {
            reply_details += ", First element type: " + std::to_string(reply->element[0]->type);
         }
    }
    throw RedisCommandException("JSON.ARRAPPEND", "Key: " + key + ", Path: " + path + ", Error: " + reply_details);
}

void RedisJSONClient::prepend_path(const std::string& key, const std::string& path,
                                   const json& value) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    std::string value_str = value.dump();

    // JSON.ARRINSERT <key> <path> <index> <json> [json ...]
    // We are prepending, so index is 0.
    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.ARRINSERT %s %s 0 %s", key.c_str(), path.c_str(), value_str.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRINSERT", "Key: " + key + ", Path: " + path + ", Index: 0, Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRINSERT", "Key: " + key + ", Path: " + path + ", Index: 0, Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.ARRINSERT returns an array of integers, similar to JSON.ARRAPPEND.
    // Each integer is the new length of the array after inserting a value.
    // If a single path and single value, returns an array with one integer (new length) or NIL if path is not an array.
    // If key does not exist, or path before last segment does not exist, returns REDIS_REPLY_NIL.

    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        throw PathNotFoundException(key, path, "Key or path does not exist for JSON.ARRINSERT");
    }

    if (reply->type == REDIS_REPLY_ARRAY) {
        if (reply->elements == 1) {
            redisReply* element_reply = reply->element[0];
            if (element_reply->type == REDIS_REPLY_INTEGER) {
                // Success, new length is element_reply->integer.
                _connection_manager->return_connection(std::move(conn));
                return;
            } else if (element_reply->type == REDIS_REPLY_NIL) {
                // Path exists but is not an array.
                _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("JSON.ARRINSERT", "Key: " + key + ", Path: " + path + " exists but is not an array.");
            }
        }
    }
    
    _connection_manager->return_connection(std::move(conn));
    std::string reply_details = "Unexpected reply type: " + std::to_string(reply->type);
    if (reply->type == REDIS_REPLY_STRING) {
        reply_details += ", Value: " + std::string(reply->str);
    } else if (reply->type == REDIS_REPLY_ARRAY) {
        reply_details += ", Elements: " + std::to_string(reply->elements);
         if (reply->elements > 0 && reply->element[0]) {
            reply_details += ", First element type: " + std::to_string(reply->element[0]->type);
         }
    }
    throw RedisCommandException("JSON.ARRINSERT", "Key: " + key + ", Path: " + path + ", Index: 0, Error: " + reply_details);
}

json RedisJSONClient::pop_path(const std::string& key, const std::string& path,
                               int index) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    // JSON.ARRPOP <key> [path [index]]
    // If path is not provided, pops from root array. If index not provided, pops last element.
    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.ARRPOP %s %s %d", key.c_str(), path.c_str(), index)
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRPOP", "Key: " + key + ", Path: " + path + ", Index: " + std::to_string(index) + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRPOP", "Key: " + key + ", Path: " + path + ", Index: " + std::to_string(index) + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.ARRPOP returns the popped JSON value as a bulk string.
    // Returns NIL if key does not exist, path is not an array, or index is out of bounds.
    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        // This could be due to various reasons: key not found, path not an array, index out of bounds.
        // Throwing PathNotFoundException might be misleading if the path exists but is not an array, or index is bad.
        // A more generic exception or a specific one for "pop failed" might be better.
        // For now, let's use a generic command exception indicating the condition.
        throw RedisCommandException("JSON.ARRPOP", "Key: " + key + ", Path: " + path + ", Index: " + std::to_string(index) + ". Failed to pop: key/path not found, path not array, or index out of bounds.");
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string result_str = reply->str;
        _connection_manager->return_connection(std::move(conn));
        try {
            return _parse_json_reply(result_str, "JSON.ARRPOP for key '" + key + "', path '" + path + "', index " + std::to_string(index));
        } catch (const JsonParsingException& e) {
            throw; // Rethrow if it's already a JsonParsingException
        } catch (const json::parse_error& e) {
            throw JsonParsingException("Failed to parse JSON.ARRPOP response for key '" + key + "', path '" + path + "', index " + std::to_string(index) + ": " + e.what() + ". Received: " + result_str);
        }
    }

    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.ARRPOP", "Key: " + key + ", Path: " + path + ", Index: " + std::to_string(index) + ", Error: Unexpected reply type from Redis: " + std::to_string(reply->type));
}

size_t RedisJSONClient::array_length(const std::string& key, const std::string& path) const {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.ARRLEN %s %s", key.c_str(), path.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRLEN", "Key: " + key + ", Path: " + path + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.ARRLEN", "Key: " + key + ", Path: " + path + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.ARRLEN returns an integer reply: the length of the array at path.
    // If the key does not exist, or path does not exist, or path is not an array, it returns NIL.
    // Some versions/docs say it returns an array of integers (lengths). For a single path, it should be a single integer or nil.
    // Let's assume the common case for a single path: either an Integer or Nil.
    // If it returns an array with one integer, we should handle that too.

    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        // PathNotFoundException or a more specific "not an array" or "length cannot be determined"
        // Throwing PathNotFound is reasonable if the path doesn't lead to an array whose length can be determined.
        throw PathNotFoundException(key, path, "Key or path does not exist, or path is not an array, for JSON.ARRLEN");
    }

    if (reply->type == REDIS_REPLY_INTEGER) {
        long long length = reply->integer;
        _connection_manager->return_connection(std::move(conn));
        if (length < 0) { // Should not happen for ARRLEN, but good check
            throw RedisCommandException("JSON.ARRLEN", "Key: " + key + ", Path: " + path + ", Error: Received negative array length.");
        }
        return static_cast<size_t>(length);
    }
    
    // Handle case where it might return an array with one element (the length or nil)
    // This is how ReJSON 2.0+ handles it for single path.
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 1) {
        redisReply* element_reply = reply->element[0];
        if (element_reply->type == REDIS_REPLY_INTEGER) {
            long long length = element_reply->integer;
            _connection_manager->return_connection(std::move(conn));
            if (length < 0) {
                 throw RedisCommandException("JSON.ARRLEN", "Key: " + key + ", Path: " + path + ", Error: Received negative array length in array reply.");
            }
            return static_cast<size_t>(length);
        } else if (element_reply->type == REDIS_REPLY_NIL) {
            // Path specified, but it's not an array (or doesn't exist at that specific sub-path)
             _connection_manager->return_connection(std::move(conn));
            throw PathNotFoundException(key, path, "Path exists but is not an array, or sub-path not found, for JSON.ARRLEN (array reply)");
        }
    }


    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.ARRLEN", "Key: " + key + ", Path: " + path + ", Error: Unexpected reply type from Redis: " + std::to_string(reply->type));
}

// --- Merge Operations ---

void RedisJSONClient::merge_json(const std::string& key, const json& patch,
                                 const MergeStrategy& strategy) {
    // NOTE: The `MergeStrategy` enum (DEEP, SHALLOW, etc.) is not directly supported by
    // the standard Redis `JSON.MERGE` command. `JSON.MERGE` always performs a deep merge
    // of objects. If different strategies are strictly required, this would necessitate
    // client-side logic (fetch, merge with custom logic, then set) or a complex Lua script.
    // For this implementation, we will use `JSON.MERGE` with its default behavior,
    // effectively ignoring the `strategy` parameter beyond potentially logging a warning
    // if it's not the default DEEP (which aligns with JSON.MERGE).

    if (strategy != MergeStrategy::DEEP) {
        // Optionally log a warning here:
        // std::cerr << "Warning: RedisJSONClient::merge_json called with strategy other than DEEP. "
        //           << "Redis JSON.MERGE performs a deep merge by default. Strategy parameter is currently ignored." << std::endl;
        // Depending on strictness, one might even throw an std::invalid_argument if only DEEP is supported.
    }

    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    std::string patch_str = patch.dump();
    
    // JSON.MERGE <key> <path> <value>
    // We assume merging into the root of the document, so path is '$'.
    // The `patch` here is the JSON value to be merged in.
    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.MERGE %s $ %s", key.c_str(), patch_str.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.MERGE returns "OK" on success.
    // It returns NIL if the key does not exist and the path is not '$' (older versions).
    // For path '$', if the key does not exist, JSON.MERGE will create it and set it to the patch value if patch is an object/array.
    // If the existing value or patch is not a JSON object, it might error or have specific behavior.
    
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        _connection_manager->return_connection(std::move(conn));
        return; // Success
    }
    
    // JSON.MERGE (on ReJSON < 2.0) could return an integer reply (0 or 1) indicating if a change was made.
    // ReJSON 2.0+ returns "OK". We will primarily check for "OK".
    // If older versions are supported, this might need adjustment.
    // For now, strict check for "OK".

    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: Unexpected reply: " + (reply->str ? reply->str : "Status not OK or unknown reply type ") + std::to_string(reply->type));
}

void RedisJSONClient::patch_json(const std::string& key, const json& patch_operations) {
    // This is a non-atomic client-side implementation of JSON Patch (RFC 6902).
    // For atomicity, a Lua script would be required, which would need a Lua JSON library
    // and a JSON Patch library in Lua, or a very complex script.
    // Alternatively, if RedisJSON supported JSON.PATCH command directly, that would be used.

    // 1. Get the current JSON document
    json current_doc;
    try {
        current_doc = get_json(key); // Uses the class's existing get_json method
    } catch (const PathNotFoundException& e) {
        // If the key does not exist, a patch operation might still be valid if it's an "add" to root.
        // However, nlohmann::json::patch expects the document to exist.
        // RFC6902 says: "If the target document is an empty JSON document (e.g., ""),
        // the "add" operation can be used to add a data structure to the document."
        // nlohmann json `patch` method expects a non-null document to patch.
        // If key doesn't exist, we can treat it as an empty object `json::object()` or based on patch.
        // For simplicity now, if key does not exist, we'll let it fail if patch expects content,
        // or let it proceed if patch creates everything (e.g. add to root of a conceptual empty doc).
        // A common approach for "add" to a non-existent root would be to initialize current_doc to json::object() or json::value_t::null.
        // Let's assume for now `get_json` throws if key not found, and we re-throw or handle.
        // If the patch is meant to create the document (e.g. add / an empty object),
        // then `get_json` failing is problematic.
        // Let's try to patch a null JSON object if the key is not found, as some patch operations
        // (like adding to the root) might still be valid.
        current_doc = json::object(); // Start with an empty object if key not found.
                                      // This might not be correct for all patch operations.
                                      // A specific "add" to "/" might work.
                                      // Consider if PathNotFoundException should be re-thrown.
                                      // For now, let's assume "patching a non-existent doc" means starting from `{}`.
                                      // This behavior might need refinement based on desired semantics.
                                      // A safer default if key not found might be to throw, unless patch is a single "add" to "/"
    } catch (const RedisException& e) { // Catch other Redis specific exceptions from get_json
        throw; // Rethrow other potentially critical errors from get_json
    }


    // 2. Apply the patch operations (RFC 6902)
    json patched_doc;
    try {
        patched_doc = current_doc.patch(patch_operations);
    } catch (const json::exception& e) {
        // This could be json::parse_error if patch_operations is malformed,
        // or json::out_of_range / json::type_error if patch operations are invalid for the current_doc.
        throw JsonPatchException("Failed to apply JSON Patch for key '" + key + "': " + e.what());
    }

    // 3. Set the patched document back to Redis
    try {
        // Use the class's existing set_json method. This doesn't provide options like NX/XX from here.
        // If TTL was associated with the key, it will be cleared by set_json unless set_json is modified
        // or we fetch TTL and re-apply. For simplicity, this basic patch doesn't manage TTL explicitly.
        set_json(key, patched_doc);
    } catch (const RedisException& e) {
        // Catch Redis specific exceptions from set_json
        throw; // Rethrow
    }
}

// --- Atomic Operations (using Lua) ---

json RedisJSONClient::atomic_get_set(const std::string& key, const std::string& path,
                                     const json& new_value) {
    if (!_lua_script_manager) {
        throw RedisConfigurationException("LuaScriptManager is not initialized.");
    }

    // Script name expected to be loaded by LuaScriptManager
    const std::string script_name = "atomic_json_get_set_path";

    // Ensure script is loaded (optional check, execute_script might throw if not found)
    // if (!_lua_script_manager->is_script_loaded(script_name)) {
    //    throw RedisConfigurationException("Lua script '" + script_name + "' is not loaded.");
    // }
    // Alternatively, LuaScriptManager::preload_builtin_scripts() should be called during client construction.

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, new_value.dump()};

    try {
        // _execute_lua_script is a private helper that should call the public LuaScriptManager method.
        // Let's call the manager directly as _execute_lua_script itself is also a TODO/stub.
        // json result = _execute_lua_script(script_name, keys, args);

        json result = _lua_script_manager->execute_script(script_name, keys, args);

        // The Lua script for RedisJSON's JSON.GET returns a string which is a JSON value,
        // or nil if path not found. LuaScriptManager::execute_script and its helper
        // redis_reply_to_json should handle parsing this string back to nlohmann::json.
        // If current_value_str was nil from redis.call, redis_reply_to_json should turn it into json(nullptr).
        return result;

    } catch (const LuaScriptException& e) {
        // More specific error context
        throw RedisLuaScriptException("Error executing atomic_get_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const JsonParsingException& e) {
        throw RedisLuaScriptException("Failed to parse result from atomic_get_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const RedisException& e) { // Catch other Redis specific exceptions
        throw; // Rethrow
    }
}

bool RedisJSONClient::atomic_compare_set(const std::string& key, const std::string& path,
                                        const json& expected, const json& new_value) {
    if (!_lua_script_manager) {
        throw RedisConfigurationException("LuaScriptManager is not initialized.");
    }

    const std::string script_name = "atomic_json_compare_set_path";
    // LuaScriptManager::preload_builtin_scripts() should load this.

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, expected.dump(), new_value.dump()};

    try {
        json result_json = _lua_script_manager->execute_script(script_name, keys, args);

        // The Lua script returns 1 for success (set was performed) or 0 for failure (no match).
        // LuaScriptManager::redis_reply_to_json should convert Redis integer reply to nlohmann::json number.
        if (result_json.is_number_integer()) {
            return result_json.get<int>() == 1;
        } else {
            // Should not happen if script behaves correctly.
            throw RedisLuaScriptException("Atomic compare-and-set script '" + script_name + "' for key '" + key + "', path '" + path +
                                          "' returned non-integer result: " + result_json.dump());
        }
    } catch (const LuaScriptException& e) {
        throw RedisLuaScriptException("Error executing atomic_compare_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const RedisException& e) { // Catch other Redis specific exceptions
        throw; // Rethrow
    }
}

// --- Utility Operations ---

std::vector<std::string> RedisJSONClient::keys_by_pattern(const std::string& pattern) const {
    std::vector<std::string> found_keys;
    std::string cursor = "0";
    std::unique_ptr<RedisConnection> conn; // Declare here for potential reuse across SCAN loops

    do {
        conn = get_redis_connection(); // Get a fresh connection for each SCAN command or reuse carefully.
                                       // For simplicity of connection management, getting a new one is safer.

        RedisReplyPtr reply(static_cast<redisReply*>(
            // Using a COUNT of 100, adjust as needed for performance/memory.
            conn->command("SCAN %s MATCH %s COUNT 100", cursor.c_str(), pattern.c_str())
        ));

        if (!reply) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: No reply or connection error");
        }

        if (reply->type == REDIS_REPLY_ERROR) {
            std::string err_msg = (reply->str ? reply->str : "Redis error reply with no message");
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: " + err_msg);
        }

        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: Unexpected reply structure from SCAN.");
        }

        // First element is the new cursor
        redisReply* cursor_reply = reply->element[0];
        if (cursor_reply->type != REDIS_REPLY_STRING) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: New cursor is not a string.");
        }
        cursor = std::string(cursor_reply->str, cursor_reply->len);

        // Second element is an array of keys
        redisReply* keys_reply = reply->element[1];
        if (keys_reply->type != REDIS_REPLY_ARRAY) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: Keys element is not an array.");
        }

        for (size_t i = 0; i < keys_reply->elements; ++i) {
            redisReply* key_element_reply = keys_reply->element[i];
            if (key_element_reply->type == REDIS_REPLY_STRING) {
                found_keys.emplace_back(key_element_reply->str, key_element_reply->len);
            }
            // Non-string elements in keys array would be strange, ignore or log.
        }
        
        _connection_manager->return_connection(std::move(conn)); // Return connection after processing reply

    } while (cursor != "0");

    return found_keys;
}

// Helper recursive function for search_by_value
void find_values_recursive(const json& current_json_doc, const json& search_value, json::array_t& found_values) {
    if (current_json_doc == search_value) {
        found_values.push_back(current_json_doc);
    }

    if (current_json_doc.is_object()) {
        for (auto& el : current_json_doc.items()) {
            find_values_recursive(el.value(), search_value, found_values);
        }
    } else if (current_json_doc.is_array()) {
        for (const auto& item : current_json_doc) {
            find_values_recursive(item, search_value, found_values);
        }
    }
    // Primitive types are handled by direct comparison at the start of the function.
}

json RedisJSONClient::search_by_value(const std::string& key, const json& search_value) const {
    // Client-side search. Fetches the whole document.
    // This can be inefficient for large documents.
    // RedisSearch or specific Lua scripts would be better for server-side searching.

    json document_to_search;
    try {
        document_to_search = get_json(key); // Fetches the document from Redis
    } catch (const PathNotFoundException& e) {
        // If the key doesn't exist, there's nothing to search.
        return json::array(); // Return an empty JSON array
    } catch (const RedisException& e) {
        throw; // Rethrow other Redis-related exceptions
    }

    json::array_t results_array;
    find_values_recursive(document_to_search, search_value, results_array);

    return json(results_array); // Convert the C++ vector of json objects to a single json object (array type)
}

// Helper recursive function for get_all_paths
void find_json_paths_recursive(const json& current_node, const std::string& current_path_str, std::vector<std::string>& paths_list) {
    // Add the path to the current node itself.
    // Depending on desired output, might not add path to leaf primitive if only paths to containers are needed.
    // For "all paths", path to everything including primitives makes sense.
    // paths_list.push_back(current_path_str); // Path to current node. This might be too verbose.
                                            // Usually paths to elements *within* objects/arrays are listed.
                                            // Let's adjust to add paths to where values are stored.

    if (current_node.is_object()) {
        if (current_node.empty() && current_path_str != "$") { // Represent path to empty object itself
             // paths_list.push_back(current_path_str); // Only if objects themselves need a path entry.
        }
        for (auto& el : current_node.items()) {
            // Escape key name if it contains special characters for robust path generation.
            // For simplicity, direct concatenation is used here. A robust solution would check el.key().
            // Example: `$.` is for root, `.` is separator. If key is `my.key`, path becomes `$.my.key` not `$.my.key`.
            // nlohmann::json path pointers are like "/a/b/0". We are generating JSONPath $.a.b[0]
            std::string child_path = current_path_str + "." + el.key();
            paths_list.push_back(child_path); // Path to the key-value pair
            find_json_paths_recursive(el.value(), child_path, paths_list);
        }
    } else if (current_node.is_array()) {
        if (current_node.empty() && current_path_str != "$") { // Represent path to empty array itself
            // paths_list.push_back(current_path_str);
        }
        for (size_t i = 0; i < current_node.size(); ++i) {
            std::string child_path = current_path_str + "[" + std::to_string(i) + "]";
            paths_list.push_back(child_path); // Path to the array element
            find_json_paths_recursive(current_node[i], child_path, paths_list);
        }
    }
    // For primitive types, the path *to* them (from their parent object/array) is already added.
    // If we wanted to list the path *of* the primitive value itself, it would be `current_path_str`.
}


std::vector<std::string> RedisJSONClient::get_all_paths(const std::string& key) const {
    // Client-side implementation. Fetches the whole document.
    // RedisJSON's `JSON.DEBUG PATHS <key> [path]` could be an alternative if available and suitable,
    // but it's a debug command and might not be universally available or intended for production use.

    json document;
    try {
        document = get_json(key);
    } catch (const PathNotFoundException& e) {
        // Key does not exist, so no paths.
        return {};
    } catch (const RedisException& e) {
        throw; // Rethrow other Redis-related errors
    }

    std::vector<std::string> all_paths;
    if (!document.is_null() && !document.empty()) { // Only proceed if there's something to get paths from
        all_paths.push_back("$"); // Path to the root element itself
        find_json_paths_recursive(document, "$", all_paths);
    } else if (document.is_object() || document.is_array()) { // Handles empty object {} or array [] at root
        all_paths.push_back("$");
    }


    // Remove duplicate paths that might arise if we add path for current node and then for its children.
    // The current recursive logic aims to avoid direct duplicates by adding path to child.
    // std::sort(all_paths.begin(), all_paths.end());
    // all_paths.erase(std::unique(all_paths.begin(), all_paths.end()), all_paths.end());
    // The current implementation should generate unique paths by construction.

    return all_paths;
}

// --- Access to sub-components ---
// These might not need implementation if they are just returning references/pointers
// to already initialized members. However, ensure members are initialized.

JSONQueryEngine& RedisJSONClient::query_engine() {
    if (!_query_engine) {
        // Lazy initialization or ensure it's done in constructor
        _query_engine = std::make_unique<JSONQueryEngine>(*this);
    }
    return *_query_engine;
}

JSONCache& RedisJSONClient::cache() {
    if (!_json_cache) {
        _json_cache = std::make_unique<JSONCache>(/* constructor args if any */);
    }
    return *_json_cache;
}

JSONSchemaValidator& RedisJSONClient::schema_validator() {
    if (!_schema_validator) {
        _schema_validator = std::make_unique<JSONSchemaValidator>(/* constructor args if any */);
    }
    return *_schema_validator;
}

JSONEventEmitter& RedisJSONClient::event_emitter() {
    if (!_event_emitter) {
        _event_emitter = std::make_unique<JSONEventEmitter>(/* constructor args if any */);
    }
    return *_event_emitter;
}

TransactionManager& RedisJSONClient::transaction_manager() {
    if (!_transaction_manager) {
        // TransactionManager might need RedisConnectionManager or similar
        _transaction_manager = std::make_unique<TransactionManager>(_connection_manager.get(), _path_parser.get(),_json_modifier.get());
    }
    return *_transaction_manager;
}


// --- Internal Helpers ---

void RedisJSONClient::_perform_write_operation(const std::string& key, const std::optional<std::string>& path, std::function<void(RedisConnection&)> operation) {
    // Internal helper. If called by stubbed methods, it might not be fully exercised.
    // Actual implementation would involve getting a connection, running operation,
    // then cache invalidation and event emission logic.
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
    try {
        operation(*conn);
        // if (_json_cache) _json_cache->invalidate(key, path);
        // if (_event_emitter) _event_emitter->emit(key, path, "modified"); // Example event
    } catch (...) {
        _connection_manager->return_connection(std::move(conn));
        throw;
    }
    _connection_manager->return_connection(std::move(conn));
    // For now, just a basic pass-through or throw if called by a stub.
    // This method itself doesn't need to throw "Not Implemented" if its callers do.
}

json RedisJSONClient::_get_document_with_caching(const std::string& key) const {
    // Internal helper.
    // if (_json_cache && _json_cache->is_enabled()) {
    //     auto cached_val = _json_cache->get(key);
    //     if (cached_val) return *cached_val;
    // }
    // json doc = get_json(key); // Careful: this calls the public get_json
                                // This helper should probably implement direct Redis GET
    // if (_json_cache && _json_cache->is_enabled()) {
    //     _json_cache->put(key, doc);
    // }
    // return doc;
    throw std::runtime_error("Not implemented: RedisJSONClient::_get_document_with_caching (via stub)");
    return json{};
}

json RedisJSONClient::_parse_json_reply(const std::string& reply_str, const std::string& context_msg) const {
    try {
        return json::parse(reply_str);
    } catch (const json::parse_error& e) {
        throw JsonParsingException(context_msg + ": " + e.what() + ". Received: " + reply_str);
    }
}


} // namespace redisjson
