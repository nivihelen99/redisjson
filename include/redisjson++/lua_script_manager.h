#pragma once

#include "redis_connection_manager.h" // Needs access to RedisConnection or similar
#include "exceptions.h"             // For LuaScriptException
#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <iostream> // For std::cerr in implementation (temporary logging)
#include <map> // For SCRIPT_DEFINITIONS

using json = nlohmann::json;

namespace redisjson {

// Forward declare RedisConnectionManager if not fully included or provide an interface
class RedisConnectionManager;

class LuaScriptManager {
public:
    // Takes a raw pointer to connection manager, does not own it.
    // Manager must outlive the script manager or be handled carefully.
    // Alternatively, could take a shared_ptr or a factory function for connections.
    explicit LuaScriptManager(RedisConnectionManager* conn_manager);
    ~LuaScriptManager();

    LuaScriptManager(const LuaScriptManager&) = delete;
    LuaScriptManager& operator=(const LuaScriptManager&) = delete;

    /**
     * Loads a Lua script into Redis and caches its SHA1 hash.
     * If the script is already loaded (based on name), this might re-load or do nothing.
     * @param name User-defined name for the script (for caching and later execution).
     * @param script_body The Lua script code.
     * @throws RedisCommandException if SCRIPT LOAD fails.
     * @throws ConnectionException if connection fails.
     */
    void load_script(const std::string& name, const std::string& script_body);

    /**
     * Executes a pre-loaded Lua script by its SHA1 hash.
     * @param name The user-defined name of the script (used to find its SHA1).
     * @param keys A vector of key names to be passed to the script (KEYS[1], KEYS[2], ...).
     * @param args A vector of argument values to be passed to the script (ARGV[1], ARGV[2], ...).
     * @return json The result from the Lua script, parsed as JSON.
     *         The script should return a JSON-compatible string or structure.
     *         Handles nil replies from Redis as json(nullptr).
     * @throws LuaScriptException if script name not found, execution fails (NOSCRIPT, runtime error).
     * @throws RedisCommandException for other Redis errors.
     * @throws ConnectionException if connection fails.
     * @throws JsonParsingException if script output cannot be parsed to JSON.
     */
    json execute_script(const std::string& name,
                        const std::vector<std::string>& keys,
                        const std::vector<std::string>& args);

    /**
     * Loads all built-in Lua scripts defined in the requirements.
     * This should be called once, perhaps during RedisJSONClient initialization.
     */
    void preload_builtin_scripts();

    /**
     * Checks if a script by the given name has been loaded (i.e., its SHA is cached).
     */
    bool is_script_loaded(const std::string& name) const;

    /**
     * Clears Redis's Lua script cache on the server (SCRIPT FLUSH) and local SHA cache.
     * Use with caution.
     */
    void clear_all_scripts_cache(); // SCRIPT FLUSH on Redis

    /**
     * Clears only the local cache of script SHAs. Redis server cache is unaffected.
     */
    void clear_local_script_cache();


private:
    RedisConnectionManager* connection_manager_; // Does not own
    std::unordered_map<std::string, std::string> script_shas_; // Maps script name to SHA1
    mutable std::mutex cache_mutex_; // Protects script_shas_

    // Helper to convert Redis reply to JSON.
    // This needs to be robust for various Redis reply types (string, integer, array, nil, error).
    json redis_reply_to_json(redisReply* reply) const;

    // Helper to get script body by name for on-demand loading
    const std::string* get_script_body_by_name(const std::string& name) const;

    // Built-in script bodies (can be quite large)
    // These could be static const strings or loaded from files/resources.
    // static const std::string JSON_GET_SET_LUA; // Old, to be reviewed/removed if not used
    // static const std::string JSON_COMPARE_SET_LUA; // Old, to be reviewed/removed if not used

    // Map to store script definitions for on-demand loading
    static const std::map<std::string, const std::string*> SCRIPT_DEFINITIONS;

    // Built-in Lua script strings
    static const std::string JSON_PATH_GET_LUA;
    static const std::string JSON_PATH_SET_LUA;
    static const std::string JSON_PATH_DEL_LUA;
    static const std::string JSON_PATH_TYPE_LUA;
    static const std::string JSON_ARRAY_APPEND_LUA;
    static const std::string JSON_ARRAY_PREPEND_LUA;
    static const std::string JSON_ARRAY_POP_LUA;
    static const std::string JSON_ARRAY_LENGTH_LUA;
    static const std::string ATOMIC_JSON_GET_SET_PATH_LUA;
    static const std::string ATOMIC_JSON_COMPARE_SET_PATH_LUA;
    // ... other built-in scripts
};

} // namespace redisjson
