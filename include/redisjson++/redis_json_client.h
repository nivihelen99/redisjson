#pragma once

#include <string>
#include <vector>
#include <memory> // For std::unique_ptr
#include <nlohmann/json.hpp>

#include "common_types.h" // For ClientConfig and SetOptions
#include "exceptions.h"
// Forward declare classes managed by unique_ptr to reduce header dependencies if possible,
// but full types are needed for std::unique_ptr unless custom deleters are used.
// For now, assume full includes are necessary and managed carefully.
#include "redis_connection_manager.h" // Needs full definition for std::unique_ptr
#include "path_parser.h"              // Needs full definition for std::unique_ptr
#include "json_modifier.h"            // Needs full definition for std::unique_ptr
#include "lua_script_manager.h"       // Needs full definition for std::unique_ptr


// Forward declaration for nlohmann::json for convenience
using json = nlohmann::json;

namespace redisjson {

// Forward declare RedisConnectionManager, PathParser, JSONModifier, LuaScriptManager, RedisConnection
// if their full definitions are not strictly needed in this header (e.g. if only pointers/references were used).
// However, std::unique_ptr members generally require the full definition of the pointed-to type.
class RedisConnectionManager;
class PathParser;
class JSONModifier;
class LuaScriptManager;
class RedisConnection; // For the return type of get_redis_connection()

class RedisJSONClient {
public:
    explicit RedisJSONClient(const ClientConfig& client_config);
    ~RedisJSONClient();

    // Document Operations
    void set_json(const std::string& key, const json& document,
                  const SetOptions& opts = {});
    json get_json(const std::string& key) const; // Mark const if it doesn't modify client state
    bool exists_json(const std::string& key) const;
    void del_json(const std::string& key);

    // Path Operations (to be implemented later)
    // json get_path(const std::string& key, const std::string& path) const;
    // void set_path(const std::string& key, const std::string& path,
    //               const json& value, const SetOptions& opts = {});
    // void del_path(const std::string& key, const std::string& path);
    // bool exists_path(const std::string& key, const std::string& path) const;

private:
    ClientConfig config_; // Store a copy of the config
    std::unique_ptr<RedisConnectionManager> connection_manager_;
    std::unique_ptr<PathParser> path_parser_;         // To be implemented
    std::unique_ptr<JSONModifier> json_modifier_;     // To be implemented
    std::unique_ptr<LuaScriptManager> lua_script_manager_; // To be implemented

    // Helper to execute a command and handle reply (basic version)
    // Returns redisReply smart pointer or throws on error.
    // std::unique_ptr<redisReply, void(*)(void*)> execute_command(const char* format, ...);
    // std::unique_ptr<redisReply, void(*)(void*)> execute_command_argv(int argc, const char **argv, const size_t *argvlen);

    // Helper to get a connection (wraps getting from manager and throwing if null)
    std::unique_ptr<RedisConnection> get_redis_connection() const;
};

} // namespace redisjson
