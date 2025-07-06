#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h" // For RedisConnection
#include <vector>

namespace redisjson {

// --- Built-in Lua Scripts Definitions ---
// These are simplified placeholders. Real scripts would be more complex.
// Example: A script that gets a JSON string, parses it (if cjson is available on server),
// modifies a path, and sets it back. For non-RedisJSON modules, server-side parsing is key.
// If cjson is not available on the Redis server, Lua scripts would have to operate on the string
// representation or fetch, modify client-side, and set (which is not atomic for complex ops).
// The requirement implies Lua scripts *are* doing JSON manipulation, so cjson.decode/encode is assumed.

// This script is a conceptual example. Actual RedisJSON module scripts are more complex.
// For a library *not* using RedisJSON module, Lua scripts for JSON ops are non-trivial.
// They'd need a Lua JSON library (like cjson) available in Redis's Lua environment.
const std::string LuaScriptManager::JSON_GET_SET_LUA = R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1] -- Simplified: path as a string, not parsed deeply here
    local new_value_json_str = ARGV[2]

    local current_json_str = redis.call('GET', key)
    if not current_json_str then
        return nil -- Or some error indication
    end

    -- Assume cjson is available in Redis Lua environment
    local current_doc = cjson.decode(current_json_str)
    local new_value = cjson.decode(new_value_json_str)

    -- Simplified path handling: assumes path_str is a single top-level key
    -- A real script would need a robust path parser in Lua or receive parsed path elements.
    local old_value_at_path = current_doc[path_str]
    current_doc[path_str] = new_value

    redis.call('SET', key, cjson.encode(current_doc))

    if old_value_at_path == nil then -- cjson might convert Lua nil to json null if that's how it works
        return redis.call('cjson.encode', nil) -- Explicitly return JSON null string
    end
    return cjson.encode(old_value_at_path)
)lua";

// Placeholder - real script would be more involved
const std::string LuaScriptManager::JSON_COMPARE_SET_LUA = R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local expected_value_json_str = ARGV[2]
    local new_value_json_str = ARGV[3]

    local current_json_str = redis.call('GET', key)
    if not current_json_str then
        return 0 -- False: key does not exist
    end

    local current_doc = cjson.decode(current_json_str)
    local expected_value = cjson.decode(expected_value_json_str)
    local new_value = cjson.decode(new_value_json_str)

    -- Simplified path handling
    local actual_value_at_path = current_doc[path_str]

    -- Note: Comparing JSON objects/arrays for equality in Lua can be tricky.
    -- This simple comparison might not work for complex types.
    -- Usually, deep comparison or comparing their string encodings is needed.
    -- For this placeholder, assume direct comparison works for simple values.
    if actual_value_at_path == expected_value then
        current_doc[path_str] = new_value
        redis.call('SET', key, cjson.encode(current_doc))
        return 1 -- True: success
    else
        return 0 -- False: comparison failed
    end
)lua";

const std::string LuaScriptManager::ATOMIC_JSON_GET_SET_PATH_LUA = R"lua(
-- KEYS[1]: key
-- ARGV[1]: path
-- ARGV[2]: new_value (JSON string)
local current_value_str = redis.call('JSON.GET', KEYS[1], ARGV[1])
redis.call('JSON.SET', KEYS[1], ARGV[1], ARGV[2])
return current_value_str
)lua";

const std::string LuaScriptManager::ATOMIC_JSON_COMPARE_SET_PATH_LUA = R"lua(
-- KEYS[1]: key
-- ARGV[1]: path
-- ARGV[2]: expected_value (JSON string)
-- ARGV[3]: new_value (JSON string)
local current_value_lua_str = redis.call('JSON.GET', KEYS[1], ARGV[1])
local expected_value_json_str = ARGV[2]
-- If path does not exist, JSON.GET returns Lua boolean false.
if current_value_lua_str == false then
    -- If expected value is the JSON literal "null", treat non-existence as matching "null".
    if expected_value_json_str == "null" then
        redis.call('JSON.SET', KEYS[1], ARGV[1], ARGV[3])
        return 1 -- Set succeeded
    else
        return 0 -- Path not found and expected was not "null"
    end
end
-- Path exists, current_value_lua_str is a JSON string (e.g., "\"foo\"", "123", "{\"a\":1}", "null")
if current_value_lua_str == expected_value_json_str then
    redis.call('JSON.SET', KEYS[1], ARGV[1], ARGV[3])
    return 1 -- Set succeeded
else
    return 0 -- Value did not match
end
)lua";


// --- LuaScriptManager Implementation ---
LuaScriptManager::LuaScriptManager(RedisConnectionManager* conn_manager)
    : connection_manager_(conn_manager) {
    if (!conn_manager) {
        throw std::invalid_argument("RedisConnectionManager cannot be null for LuaScriptManager.");
    }
}

LuaScriptManager::~LuaScriptManager() {
    // Local SHA cache (script_shas_) is cleaned up by its destructor.
    // No ownership of connection_manager_.
}

void LuaScriptManager::load_script(const std::string& name, const std::string& script_body) {
    if (name.empty() || script_body.empty()) {
        throw std::invalid_argument("Script name and body cannot be empty.");
    }

    auto conn_guard = connection_manager_->get_connection(); // This will throw if simplified get_connection fails
    RedisConnection* conn = conn_guard.get();
    if (!conn || !conn->is_connected()) {
        throw ConnectionException("Failed to get valid Redis connection for SCRIPT LOAD.");
    }

    redisReply* reply = conn->command("SCRIPT LOAD %s", script_body.c_str());

    if (!reply) {
        throw RedisCommandException("SCRIPT LOAD", "No reply from Redis (connection error: " + (conn->get_context() ? std::string(conn->get_context()->errstr) : "unknown") + ")");
    }

    std::string sha1_hash;
    if (reply->type == REDIS_REPLY_STRING) {
        sha1_hash = std::string(reply->str, reply->len);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = std::string(reply->str, reply->len);
        freeReplyObject(reply);
        throw RedisCommandException("SCRIPT LOAD", err_msg);
    } else {
        // Unexpected reply type
        freeReplyObject(reply);
        throw RedisCommandException("SCRIPT LOAD", "Unexpected reply type: " + std::to_string(reply->type));
    }

    freeReplyObject(reply);

    if (sha1_hash.empty()) {
         throw RedisCommandException("SCRIPT LOAD", "Failed to load script, SHA1 hash is empty.");
    }

    std::lock_guard<std::mutex> lock(cache_mutex_);
    script_shas_[name] = sha1_hash;
}

json LuaScriptManager::redis_reply_to_json(redisReply* reply) const {
    if (!reply) return json(nullptr); // Or throw? Depending on context.

    switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS: // Status often contains simple strings like "OK"
            // Attempt to parse as JSON string. If script returns plain string, it might be an error or simple value.
            // For scripts designed to return JSON, the string reply *is* the JSON.
            try {
                return json::parse(reply->str, reply->str + reply->len);
            } catch (const json::parse_error& e) {
                // If it's not valid JSON, but a simple string, what to do?
                // For now, assume scripts always return valid JSON strings or handle errors via REDIS_REPLY_ERROR.
                // If a script is meant to return a non-JSON string, this needs adjustment.
                // Or, it could be a JSON string that happens to be simple, like "\"OK\"".
                // Let's be strict: if it's a string reply from EVALSHA, it should be a JSON string.
                throw JsonParsingException("Failed to parse script string output as JSON: " + std::string(e.what()) + ", content: " + std::string(reply->str, reply->len));
            }
        case REDIS_REPLY_INTEGER:
            return json(reply->integer); // Convert integer to JSON number
        case REDIS_REPLY_NIL:
            return json(nullptr); // JSON null
        case REDIS_REPLY_ARRAY: {
            // Lua scripts can return tables, which hiredis maps to REDIS_REPLY_ARRAY.
            // Each element of the array also needs conversion.
            // This is important if a script returns a list of JSON strings or mixed types.
            json::array_t arr;
            for (size_t i = 0; i < reply->elements; ++i) {
                arr.push_back(redis_reply_to_json(reply->element[i]));
            }
            return arr;
        }
        case REDIS_REPLY_ERROR:
            // This is an error FROM THE SCRIPT ITSELF (e.g. Lua runtime error)
            // or Redis error like OOM.
            throw LuaScriptException("<unknown script, error from reply>", std::string(reply->str, reply->len));
        default:
            throw RedisCommandException("EVALSHA/SCRIPT", "Unexpected Redis reply type: " + std::to_string(reply->type));
    }
}


json LuaScriptManager::execute_script(const std::string& name,
                                    const std::vector<std::string>& keys,
                                    const std::vector<std::string>& args) {
    std::string sha1_hash;
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = script_shas_.find(name);
        if (it == script_shas_.end()) {
            throw LuaScriptException(name, "Script not loaded or SHA not found in local cache.");
        }
        sha1_hash = it->second;
    }

    auto conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();
     if (!conn || !conn->is_connected()) {
        throw ConnectionException("Failed to get valid Redis connection for EVALSHA.");
    }

    // Construct argv for redisCommandArgv: "EVALSHA", sha1, num_keys, key1, key2, ..., arg1, arg2, ...
    std::vector<const char*> argv_c;
    std::vector<size_t> argv_len;

    argv_c.push_back("EVALSHA");
    argv_len.push_back(strlen("EVALSHA"));

    argv_c.push_back(sha1_hash.c_str());
    argv_len.push_back(sha1_hash.length());

    std::string num_keys_str = std::to_string(keys.size());
    argv_c.push_back(num_keys_str.c_str());
    argv_len.push_back(num_keys_str.length());

    for (const auto& key : keys) {
        argv_c.push_back(key.c_str());
        argv_len.push_back(key.length());
    }
    for (const auto& arg : args) {
        argv_c.push_back(arg.c_str());
        argv_len.push_back(arg.length());
    }

    redisReply* reply = conn->command_argv(argv_c.size(), argv_c.data(), argv_len.data());

    if (!reply) {
         throw RedisCommandException("EVALSHA", "No reply from Redis (connection error: " + (conn->get_context() ? std::string(conn->get_context()->errstr) : "unknown") + ")");
    }

    // Handle NOSCRIPT error: try to reload the script and retry ONCE.
    if (reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, "NOSCRIPT", 8) == 0) {
        freeReplyObject(reply);
        reply = nullptr; // Important: nullify before potential re-assignment

        // Attempt to find the original script body to reload.
        // This requires storing original scripts if we want to auto-reload.
        // For now, assume preload_builtin_scripts or manual load_script was called.
        // If the script was ad-hoc loaded and not built-in, we can't easily get its body here
        // unless we also cache script bodies by name (which script_shas_ doesn't do).
        // This is a limitation of the current design if scripts can be flushed from Redis.

        // Simplification: For now, do not attempt auto-reload. Client should ensure scripts are loaded.
        // Or, `load_script` could be more intelligent (SCRIPT EXISTS before LOAD).
        // A robust solution would re-load the script using its original body (if cached by LuaScriptManager)
        // and then retry EVALSHA.

        // For this iteration, NOSCRIPT is a fatal error for this execution attempt.
        // {
        //    std::lock_guard<std::mutex> lock(cache_mutex_);
        //    script_shas_.erase(name); // Remove potentially stale SHA
        // }
        // throw LuaScriptException(name, "Script not found on server (NOSCRIPT). Consider reloading.");

        // Let's try to re-load if it's a known built-in script (conceptual - needs script body access)
        // This part is tricky as we need the script body.
        // For now, just throw:
        std::string noscript_error = std::string(reply->str, reply->len);
        freeReplyObject(reply);
        throw LuaScriptException(name, "Script not found on server (NOSCRIPT): " + noscript_error + ". Reload script and retry.");
    }

    json result_json;
    try {
        result_json = redis_reply_to_json(reply); // This can throw LuaScriptException for script runtime errors
    } catch (const LuaScriptException& e) { // Catch script error from redis_reply_to_json
        freeReplyObject(reply);
        throw LuaScriptException(name, "Error during script execution: " + std::string(e.what()));
    } catch (...) { // Catch other parsing errors etc.
        freeReplyObject(reply);
        throw;
    }

    freeReplyObject(reply);
    return result_json;
}


void LuaScriptManager::preload_builtin_scripts() {
    // Example of preloading. In a real app, script bodies would be more robustly managed.
    try {
        // Load new scripts for RedisJSON path operations
        load_script("atomic_json_get_set_path", ATOMIC_JSON_GET_SET_PATH_LUA);
        load_script("atomic_json_compare_set_path", ATOMIC_JSON_COMPARE_SET_PATH_LUA);

        // Reviewing old scripts:
        // The old JSON_GET_SET_LUA and JSON_COMPARE_SET_LUA operate on full key GET/SET
        // and assume Lua-side JSON parsing (cjson) for path-like operations.
        // These might be useful for a generic Redis client but are less ideal for a RedisJSON specific client
        // if path operations are intended to use RedisJSON's native path support.
        // For now, I will comment them out from preloading to avoid confusion,
        // unless they are explicitly needed for other functionalities not covered by current TODOs.
        // load_script("json_get_set", JSON_GET_SET_LUA);
        // load_script("json_compare_set", JSON_COMPARE_SET_LUA);
        // ... load other built-in scripts if any ...
        // load_script("json_merge.lua", JSON_MERGE_LUA); // Example, if a Lua based merge was needed
        // load_script("json_array_ops.lua", JSON_ARRAY_OPS_LUA); // Example
        // load_script("json_search.lua", JSON_SEARCH_LUA);

    } catch (const RedisJSONException& e) {
        // Failed to preload one or more scripts. This could be a critical setup error.
        // Log this error. Depending on policy, may rethrow or continue.
        // For now, rethrow as it might leave the client in an inconsistent state.
        throw LuaScriptException("preload_builtin_scripts", "Failed to load a built-in script: " + std::string(e.what()));
    }
}

bool LuaScriptManager::is_script_loaded(const std::string& name) const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return script_shas_.count(name) > 0;
}

void LuaScriptManager::clear_all_scripts_cache() {
    auto conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();
    if (!conn || !conn->is_connected()) {
        throw ConnectionException("Failed to get valid Redis connection for SCRIPT FLUSH.");
    }

    redisReply* reply = conn->command("SCRIPT FLUSH");
    if (!reply) {
         throw RedisCommandException("SCRIPT FLUSH", "No reply from Redis (connection error: " + (conn->get_context() ? std::string(conn->get_context()->errstr) : "unknown") + ")");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = std::string(reply->str, reply->len);
        freeReplyObject(reply);
        throw RedisCommandException("SCRIPT FLUSH", err_msg);
    }
    // Typically SCRIPT FLUSH returns "OK" on success
    if (!(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0)) {
         // Log unexpected success reply, but proceed to clear local cache
    }

    freeReplyObject(reply);

    // Clear local cache as well
    clear_local_script_cache();
}

void LuaScriptManager::clear_local_script_cache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    script_shas_.clear();
}

} // namespace redisjson
