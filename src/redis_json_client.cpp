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
    // Consider calling lua_script_manager_->preload_builtin_scripts(); here if appropriate
    // For now, user might need to call it explicitly or it's done lazily.
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

} // namespace redisjson
