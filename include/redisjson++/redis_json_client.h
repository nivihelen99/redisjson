#pragma once

#include <string>
#include <vector>
#include <memory> // For std::unique_ptr
#include <nlohmann/json.hpp>

#include "redis_connection_manager.h"
#include "path_parser.h"       // Assuming PathParser will be created
#include "json_modifier.h"     // Assuming JSONModifier will be created
#include "lua_script_manager.h"// Assuming LuaScriptManager will be created
#include "exceptions.h"

// Forward declaration for nlohmann::json for convenience
using json = nlohmann::json;

namespace redisjson {

// Defined in requirement.md, might move to a common types header
struct SetOptions {
    bool create_path = true;        // Create intermediate paths (more relevant for set_path)
    bool overwrite = true;          // Overwrite existing values
    std::chrono::seconds ttl = std::chrono::seconds(0); // TTL (0 = no expiry)
    // bool compress = false;          // Compress large JSON (future feature)
    // bool validate_schema = false;   // Validate against schema (future feature)
    // std::string schema_name = "";   // Schema name for validation (future feature)
    // bool emit_events = true;        // Emit change events (future feature)
    // int retry_count = 3;            // Number of retries on failure (client might handle this)
};


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
