#include "redisjson++/redis_json_client.h"
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr (used in legacy mode)
#include "redisjson++/exceptions.h"
#include "redisjson++/redis_connection_manager.h" // For legacy mode
#include "redisjson++/lua_script_manager.h"      // For legacy mode

#include <stdexcept>
#include <string>
#include <iostream>
#include <thread>
#include <cstring> // For strcmp

// Note: Forward declaration of redisContext for the fake DBConnector if actual headers are not found
// This is not ideal but helps with isolated compilation if the environment is not fully set up.
// #ifndef HIREDIS_H  // Removed as hiredis.h should be included via redis_json_client.h
// struct redisContext {};
// struct redisReply {};
// #endif


namespace redisjson {

// Constructor for legacy direct Redis connections
RedisJSONClient::RedisJSONClient(const LegacyClientConfig& client_config)
    : _is_swss_mode(false), _legacy_config(client_config) {
    _connection_manager = std::make_unique<RedisConnectionManager>(_legacy_config);
    _path_parser = std::make_unique<PathParser>();
    _json_modifier = std::make_unique<JSONModifier>();
    _lua_script_manager = std::make_unique<LuaScriptManager>(_connection_manager.get());
    if (_lua_script_manager) {
        try {
            _lua_script_manager->preload_builtin_scripts();
        } catch (const RedisJSONException& e) {
            throw RedisJSONException("Failed to preload Lua scripts during RedisJSONClient construction: " + std::string(e.what()));
        }
    }
    // Initialize other legacy components if they were kept:
    // _query_engine = std::make_unique<JSONQueryEngine>(*this);
    // _transaction_manager = std::make_unique<TransactionManager>(...);
}

// Constructor for SONiC SWSS environment
RedisJSONClient::RedisJSONClient(const SwssClientConfig& swss_config)
    : _is_swss_mode(true), _swss_config(swss_config) {
    // Initialize DBConnector for SWSS mode
    // The DBConnector constructor might take (db_name, timeout, waitForDb, unixPath)
    // or (db_id, timeout, waitForDb, unixPath)
    // For simplicity, assuming string db_name is preferred if available.
    // Actual SWSS DBConnector might throw on connection failure.
    try {
        // Attempt to connect using db_name string first
        _db_connector = std::make_unique<swss::DBConnector>(
            _swss_config.db_name,
            _swss_config.operation_timeout_ms,
            _swss_config.wait_for_db,
            _swss_config.unix_socket_path
        );
    } catch (const std::exception& e) { // Catch potential errors from DBConnector constructor
        throw ConnectionException("SWSS DBConnector failed to initialize for DB '" + _swss_config.db_name + "': " + e.what());
    }

    // Initialize components common to both modes or adapted for SWSS
    _path_parser = std::make_unique<PathParser>();
    _json_modifier = std::make_unique<JSONModifier>();

    // LuaScriptManager is not used in SWSS mode due to lack of EVAL support in DBConnector.
    // Other components like JSONQueryEngine, JSONCache, etc., would need specific SWSS adaptations
    // or be removed if their functionality relied heavily on direct Redis/Lua features.
}

RedisJSONClient::~RedisJSONClient() {
    // unique_ptrs will handle cleanup
}

RedisConnectionManager::RedisConnectionPtr RedisJSONClient::get_legacy_redis_connection() const {
    if (_is_swss_mode || !_connection_manager) {
        throw RedisJSONException("Legacy connection manager not available in SWSS mode or not initialized.");
    }
    try {
        return _connection_manager->get_connection();
    } catch (const ConnectionException& e) {
        throw;
    }
}

std::optional<size_t> RedisJSONClient::object_length(const std::string& key, const std::string& path) {
    if (_is_swss_mode) {
        // Client-side implementation for SWSS mode:
        json doc;
        try {
            doc = get_json(key); // Throws PathNotFoundException if key doesn't exist
        } catch (const PathNotFoundException&) {
            return std::nullopt; // Key not found
        }

        json target_node = doc;
        if (path != "$" && path != "" && path != ".") {
            try {
                target_node = _json_modifier->get(doc, _path_parser->parse(path));
            } catch (const PathNotFoundException&) {
                return std::nullopt; // Path not found within the document
            } catch (const json::exception& e) {
                throw InvalidPathException("Error accessing path '" + path + "' in key '" + key + "' for object_length (SWSS): " + e.what());
            }
        }

        if (target_node.is_object()) {
            return target_node.size(); // nlohmann::json::size() gives number of keys for objects
        } else {
            // Path does not point to an object, or path was root and root is not an object
            return std::nullopt;
        }
    } else { // Legacy mode (Lua script)
        if (!_lua_script_manager) {
            throw RedisJSONException("LuaScriptManager not initialized for object_length.");
        }

        try {
            json result = _lua_script_manager->execute_script("json_object_length", {key}, {path});

            if (result.is_null()) {
                // Lua script returns nil if key/path not found, or if target is not an object (based on script logic).
                return std::nullopt;
            }
            if (result.is_number_integer()) {
                long long count = result.get<long long>();
                if (count < 0) { // Should not happen with a correct script
                    throw RedisCommandException("json_object_length", "Lua script returned negative count for key '" + key + "', path '" + path + "'.");
                }
                return static_cast<size_t>(count);
            }
            // If the script returns an error string (e.g. "ERR_TYPE Path value is an array..."),
            // LuaScriptManager::redis_reply_to_json is expected to throw LuaScriptException.
            // If it's not null and not an integer, it's an unexpected return type from the script.
            throw RedisCommandException("json_object_length", "Unexpected reply format from Lua script for key '" + key + "', path '" + path + "'. Expected integer or null, got: " + result.dump());
        } catch (const LuaScriptException& e) {
            // This catches errors like malformed JSON, or script-level errors explicitly thrown by redis.error_reply()
            // that were not handled as specific return values (like nil for not found).
            // If the script returns an error string like "ERR_TYPE...", it will be caught here.
            // We can choose to map certain Lua errors to std::nullopt or rethrow.
            // For now, rethrow, as LuaScriptException indicates a problem.
            // The Lua script for OBJLEN is designed to return an error reply for "not an object" or "not an array".
            // These will be caught as LuaScriptException.
            // It returns nil for "key not found" or "path not found".
            // So, if it's a LuaScriptException here, it's likely a type error from the script.
            // It might be better to return nullopt for type errors too, to match RedisJSON behavior more closely in some cases.
            // However, the current Lua script for OBJLEN returns redis.error_reply for type mismatches.
            // Let's refine this: if the exception message contains "ERR_TYPE" or "ERR_DECODE" or "ERR_PATH", it implies an issue
            // that isn't just "not an object suitable for OBJLEN".
            // For now, rethrowing is safer. If the script returns an error for "not an object", that's what the user gets.
            // If we want specific client-side handling for "not an object", the Lua script should return a specific non-error value
            // that the C++ side can interpret as "not an object".
            // Given current Lua script returns redis.error_reply for type mismatch, rethrowing LuaScriptException is correct.
            throw;
        }
    }
}

// --- Numeric Operations ---
json RedisJSONClient::json_numincrby(const std::string& key, const std::string& path, double value) {
    if (_is_swss_mode) {
        // Non-atomic get-modify-set for SWSS mode
        json doc = _get_document_for_modification(key); // Creates empty {} if key not found
        json current_value_at_path = json(nullptr);
        bool path_existed = true;
        std::vector<PathParser::PathElement> parsed_path = _path_parser->parse(path);

        try {
            current_value_at_path = _json_modifier->get(doc, parsed_path);
        } catch (const PathNotFoundException&) {
            path_existed = false; // current_value_at_path remains null
        }

        if (!path_existed && !current_value_at_path.is_number()) { // Path must exist and be a number, or be creatable as number
             // If path doesn't exist, we can't increment. RedisJSON errors if path doesn't exist.
            throw PathNotFoundException(key, path);
        }
        if (path_existed && !current_value_at_path.is_number()) {
            throw TypeMismatchException(path, "number", current_value_at_path.type_name());
        }

        double new_numeric_value = (path_existed ? current_value_at_path.get<double>() : 0.0) + value;

        // nlohmann::json will store double. If it's an integer, it will be represented as such (e.g., 1.0 becomes 1)
        json new_json_value = new_numeric_value;

        SetOptions opts; // Default set options
        // For NUMINCRBY, path should exist. create_path=false. Overwrite is true.
        _json_modifier->set(doc, parsed_path, new_json_value, false /*create_path*/, true /*overwrite*/);
        _set_document_after_modification(key, doc, opts);
        return new_json_value;

    } else { // Legacy mode (atomic via Lua)
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        const std::string script_name = "json_numincrby";
        std::vector<std::string> keys = {key};
        // Convert double to string with sufficient precision. std::to_string might not be enough.
        // Using nlohmann::json to dump the number ensures it's JSON-compatible.
        std::vector<std::string> args = {path, json(value).dump()};
        try {
            // The Lua script is expected to return the new value, JSON encoded.
            // LuaScriptManager::execute_script already parses this JSON string into a nlohmann::json object.
            return _lua_script_manager->execute_script(script_name, keys, args);
        } catch (const LuaScriptException& e) {
            // Lua script itself might return specific errors like "ERR_NOKEY", "ERR_NOPATH", "ERR_TYPE"
            // These are wrapped in LuaScriptException.
            throw; // Re-throw the exception as it contains the specific error from script.
        }
    }
}


// --- Document Operations ---

void RedisJSONClient::set_json(const std::string& key, const json& document, const SetOptions& opts) {
    std::string doc_str = document.dump();

    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        try {
            // Construct command parts for SET [NX|XX] [EX seconds]
            // SWSS DBConnector's set method might not directly support NX/XX/EX.
            // If it's a simple SET wrapper, we might need raw commands or separate methods.
            // Assuming a basic _db_connector->set(key, value) for now.
            // TTL and conditions need to be handled.

            // Redis command: SET key value [NX|XX] [EX seconds]
            // We need to build this command if DBConnector doesn't abstract it well.
            // For now, let's assume DBConnector's `set` is a simple HSET if table-based,
            // or a simple SET if key-value based.
            // Given our design to use top-level keys for JSON docs:

            if (opts.ttl.count() > 0) {
                if (opts.condition == SetCmdCondition::NX) {
                     // SET key value EX <ttl> NX
                    _db_connector->set(key, doc_str, "SET", "EX " + std::to_string(opts.ttl.count()) + " NX"); // Fictitious extended API for set
                } else if (opts.condition == SetCmdCondition::XX) {
                    // SET key value EX <ttl> XX
                     _db_connector->set(key, doc_str, "SET", "EX " + std::to_string(opts.ttl.count()) + " XX"); // Fictitious
                } else {
                    // SET key value EX <ttl>
                    _db_connector->set(key, doc_str, "SETEX", std::to_string(opts.ttl.count())); // SETEX key seconds value
                }
            } else {
                 if (opts.condition == SetCmdCondition::NX) {
                    // SET key value NX
                    _db_connector->set(key, doc_str, "SET", "NX"); // Fictitious
                } else if (opts.condition == SetCmdCondition::XX) {
                    // SET key value XX
                     _db_connector->set(key, doc_str, "SET", "XX"); // Fictitious
                } else {
                    // SET key value
                    _db_connector->set(key, doc_str);
                }
            }
            // The above assumes _db_connector->set can take additional arguments for Redis command modifiers.
            // A more realistic DBConnector might only offer basic set(key, value).
            // If so, raw commands would be needed:
            // e.g., _db_connector->command("SET %s %s EX %lld NX", key.c_str(), doc_str.c_str(), opts.ttl.count());
            // This needs verification against actual DBConnector API.
            // For now, we'll use the simpler set and acknowledge limitations in README.
            // Let's simplify to what a basic DBConnector might offer:
            // _db_connector->set(key, doc_str); // This loses TTL and conditions.
            // To support TTL and conditions, we MUST assume DBConnector can pass them or use raw commands.
            // Sticking to the plan of using the most direct interpretation of DBConnector for now.
            // A simple set(key,value) is most probable for DBConnector.
            // If DBConnector is very basic (only key,value SET):
            if (opts.ttl.count() > 0 || opts.condition != SetCmdCondition::NONE) {
                // We'd ideally use raw commands here if DBConnector supports it.
                // For now, let's assume the fake DBConnector's `set` can handle simple ops or we simplify features.
                // Simplified:
                 _db_connector->set(key, doc_str);
                 // TODO: Add clear note in README_swss.md that TTL/Conditions on set_json might be ignored if underlying DBConnector is basic.
            } else {
                _db_connector->set(key, doc_str);
            }

        } catch (const std::exception& e) {
            throw RedisCommandException("SET (SWSS)", "Key: " + key + ", Error: " + e.what());
        }
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply;
        std::vector<const char*> argv;
        std::vector<size_t> argvlen;

        argv.push_back("SET");
        argv.push_back(key.c_str());
        argv.push_back(doc_str.c_str());

        if (opts.ttl.count() > 0) {
            argv.push_back("EX");
            std::string ttl_str = std::to_string(opts.ttl.count());
            // Need to store ttl_str somewhere its c_str() remains valid
            static thread_local std::string static_ttl_str;
            static_ttl_str = ttl_str;
            argv.push_back(static_ttl_str.c_str());
        }
        if (opts.condition == SetCmdCondition::NX) {
            argv.push_back("NX");
        } else if (opts.condition == SetCmdCondition::XX) {
            argv.push_back("XX");
        }

        for(const char* s : argv) {
            argvlen.push_back(strlen(s));
        }

        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command_argv(argv.size(), argv.data(), argvlen.data())
        ));

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            // If NX/XX condition not met, Redis returns NIL, not error.
            if (reply && reply->type == REDIS_REPLY_NIL && opts.condition != SetCmdCondition::NONE) {
                 // This is an expected outcome for NX/XX, not an error.
            } else {
                _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("SET", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Unknown Redis error") : "No reply or connection error"));
            }
        } else if (reply->type == REDIS_REPLY_NIL && opts.condition != SetCmdCondition::NONE) {
            // Condition not met, success in terms of command execution
        } else if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") != 0) {
             _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SET", "Key: " + key + ", SET command did not return OK: " + std::string(reply->str));
        }
        _connection_manager->return_connection(std::move(conn));
    }
}

json RedisJSONClient::get_json(const std::string& key) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        try {
            std::string doc_str = _db_connector->get(key);
            if (doc_str.empty()) { // Assuming empty string means key not found for DBConnector::get
                                   // This needs to be confirmed with actual SWSS API. Redis returns nil.
                                   // A better DBConnector might return std::optional<std::string> or throw.
                throw PathNotFoundException(key, "$ (root)");
            }
            return _parse_json_reply(doc_str, "SWSS GET for key '" + key + "'");
        } catch (const PathNotFoundException&) {
            throw;
        } catch (const std::exception& e) { // Catch other potential exceptions from DBConnector
            throw RedisCommandException("GET (SWSS)", "Key: " + key + ", Error: " + e.what());
        }
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("GET %s", key.c_str())));

        if (!reply) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("GET", "Key: " + key + ", Error: No reply or connection error");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("GET", "Key: " + key + ", Error: " + (reply->str ? reply->str : "Redis error reply"));
        }
        if (reply->type == REDIS_REPLY_NIL) {
            _connection_manager->return_connection(std::move(conn));
            throw PathNotFoundException(key, "$ (root)");
        }
        if (reply->type == REDIS_REPLY_STRING) {
            std::string doc_str = reply->str;
            _connection_manager->return_connection(std::move(conn));
            return _parse_json_reply(doc_str, "GET for key '" + key + "'");
        }
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("GET", "Key: " + key + ", Error: Unexpected reply type " + std::to_string(reply->type));
    }
}

bool RedisJSONClient::exists_json(const std::string& key) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        try {
            return _db_connector->exists(key);
        } catch (const std::exception& e) {
            throw RedisCommandException("EXISTS (SWSS)", "Key: " + key + ", Error: " + e.what());
        }
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("EXISTS %s", key.c_str())));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("EXISTS", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Unknown Redis error") : "No reply"));
        }
        bool exists = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
        _connection_manager->return_connection(std::move(conn));
        return exists;
    }
}

void RedisJSONClient::del_json(const std::string& key) {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        try {
            _db_connector->del(key); // Assuming del returns void or count, not checked here for basic del
        } catch (const std::exception& e) {
            throw RedisCommandException("DEL (SWSS)", "Key: " + key + ", Error: " + e.what());
        }
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("DEL %s", key.c_str())));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("DEL", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Unknown Redis error") : "No reply"));
        }
        _connection_manager->return_connection(std::move(conn));
    }
}

// --- Helper for client-side path modifications ---
json RedisJSONClient::_get_document_for_modification(const std::string& key) const {
    // This simply calls get_json. If key doesn't exist, get_json throws PathNotFoundException.
    // For some path operations (like set_path with create_path=true), we might want to start
    // with an empty document if the key doesn't exist.
    try {
        return get_json(key);
    } catch (const PathNotFoundException& ) {
        // If the path is root '$' or if create_path is intended, return an empty object or array
        // This depends on what kind of structure the path implies at root.
        // For simplicity, if get_json fails (key not found), we start with an empty object.
        // This allows set_path to create the document.
        return json::object();
    }
}

void RedisJSONClient::_set_document_after_modification(const std::string& key, const json& document, const SetOptions& opts) {
    // This simply calls set_json.
    set_json(key, document, opts);
}


// --- Path Operations (Client-Side for SWSS) ---

json RedisJSONClient::get_path(const std::string& key, const std::string& path_str) const {
    if (path_str == "$" || path_str == ".") { // Requesting the whole document
        return get_json(key);
    }
    if (_is_swss_mode) {
        json doc = get_json(key); // PathNotFoundException if key itself doesn't exist
        // Use nlohmann/json pointer for path access. PathParser might be used to validate/convert path format.
        // Assuming path_str is compatible with nlohmann json_pointer or can be converted.
        // For simplicity, direct use, assuming PathParser can produce compatible pointer string if needed.
        // Example: "contact.email" -> "/contact/email"
        // For now, assume _path_parser can give a nlohmann::json::json_pointer
        // This part needs careful implementation of path parsing to json_pointer.
        // Let's use a simplified direct interpretation for now if PathParser is basic.
        // A proper PathParser should convert "a.b[0]" to "/a/b/0".
        try {
            // This is a simplification. A real PathParser would be needed here.
            // For now, assuming the path is directly usable or needs minor tweaks.
            // json::json_pointer ptr(_path_parser->to_json_pointer(path_str));
            // return doc.at(ptr);
            // The above is the ideal. For now, a simpler stub based on current PathParser abilities.
            // The current PathParser is likely for ReJSON paths, not json_pointer.
            // This section WILL require significant work on PathParser or assumptions.
            // Let's assume for now a very basic path evaluation if possible or throw not implemented.
            // Reverting to: get the whole doc, let user parse with nlohmann.
            // The spirit of get_path is to extract a sub-element.
            // This is a core functionality that needs to be preserved.
             json current_doc = get_json(key);
             // We need to use _json_modifier or similar to apply the path.
             // _json_modifier->get_value_at_path(current_doc, path_str);
             // This needs _json_modifier to be implemented correctly.
             // For this step, let's assume _json_modifier can do this.
            return _json_modifier->get(current_doc, _path_parser->parse(path_str));

        } catch (const json::out_of_range& e) { // nlohmann throws this if path not found
            throw PathNotFoundException(key, path_str);
        } catch (const json::parse_error& e) { // If path string is invalid for json_pointer
            throw InvalidPathException(path_str + " (JSON pointer parse error: " + e.what() + ")");
        } catch (const PathNotFoundException&) { // If underlying get_json failed
            throw;
        }
    } else { // Legacy mode (Lua based)
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str};
        try {
            json result = _lua_script_manager->execute_script("json_path_get", keys, args);
            if (result.is_array() && result.empty()) throw PathNotFoundException(key, path_str);
            return result; // Lua script returns array, first element is the actual value or array of values
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_path_get", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
    // Fallback to satisfy compiler, should be unreachable due to boolean _is_swss_mode
    throw RedisJSONException("Internal logic error: get_path reached end of function unexpectedly for key '" + key + "', path '" + path_str + "'.");
}

void RedisJSONClient::set_path(const std::string& key, const std::string& path_str,
                               const json& value, const SetOptions& opts) {
    if (path_str == "$" || path_str == ".") { // Setting the whole document
        set_json(key, value, opts);
        return;
    }
    if (_is_swss_mode) {
        json doc = _get_document_for_modification(key); // Starts with {} if key not found
        try {
            // _json_modifier->set_value_at_path(doc, path_str, value, opts.create_path);
            // This needs _json_modifier to be implemented correctly.
            _json_modifier->set(doc, _path_parser->parse(path_str), value, opts.create_path); // Assuming overwrite=true by default in modifier's set
            _set_document_after_modification(key, doc, opts);
        } catch (const json::exception& e) { // Catch errors from nlohmann during modification
            throw RedisCommandException("SET_PATH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        } catch (const PathNotFoundException& e) { // from _get_document if create_path was false and path was deep
             throw; // Rethrow if modification logic itself determines path cannot be created
        }
    } else { // Legacy mode
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::string value_dump = value.dump();
        std::string condition_str;
        switch (opts.condition) {
            case SetCmdCondition::NX: condition_str = "NX"; break;
            case SetCmdCondition::XX: condition_str = "XX"; break;
            default: condition_str = "NONE"; break;
        }
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, value_dump, condition_str, std::to_string(opts.ttl.count()), opts.create_path ? "true" : "false"};
        try {
            json result = _lua_script_manager->execute_script("json_path_set", keys, args);
            // Check result for success/condition not met (as in original code)
            if ((result.is_boolean() && !result.get<bool>()) || result.is_null()) {
                // Condition not met or script indicates no-op, not an error
                return;
            }
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_path_set", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}

void RedisJSONClient::del_path(const std::string& key, const std::string& path_str) {
    if (path_str == "$" || path_str == ".") {
        del_json(key); // Delete the whole document
        return;
    }
     if (_is_swss_mode) {
        // Get-modify-set
        SetOptions opts; // Default set options (no TTL, no conditions for the final SET)
        json doc;
        try {
            doc = get_json(key); // If key doesn't exist, this throws PathNotFound, which is fine for DEL.
        } catch (const PathNotFoundException&) {
            return; // Key or path doesn't exist, nothing to delete.
        }

        try {
            // _json_modifier->delete_value_at_path(doc, path_str);
            _json_modifier->del(doc, _path_parser->parse(path_str));
            // If del_path modified the document to be empty (e.g. last element of root object deleted)
            // some Redis implementations might delete the key. Here we just set it.
            // If doc becomes empty json object {} or array [], it will be stored as such.
            _set_document_after_modification(key, doc, opts);
        } catch (const PathNotFoundException& ) {
            return; // Path did not exist within the document, effectively a no-op for DEL.
        } catch (const json::exception& e) {
            throw RedisCommandException("DEL_PATH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str};
        try {
            _lua_script_manager->execute_script("json_path_del", keys, args); // Result indicates count, ignore for void
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_path_del", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}

bool RedisJSONClient::exists_path(const std::string& key, const std::string& path_str) const {
    if (path_str == "$" || path_str == ".") {
        return exists_json(key);
    }
    if (_is_swss_mode) {
        try {
            json doc = get_json(key); // PathNotFoundException if key itself doesn't exist
            // Use _json_modifier to check path existence within the document
            return _json_modifier->exists(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            return false; // Key itself doesn't exist, so path cannot exist.
        } catch (const json::exception& e) { // Errors during path parsing by modifier
             throw InvalidPathException("Error checking path existence for path '" + path_str + "': " + std::string(e.what()));
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str};
        try {
            json result = _lua_script_manager->execute_script("json_path_type", keys, args);
            return !result.is_null(); // Lua script returns type string or nil
        } catch (const LuaScriptException& e) {
            // If json_path_type script itself errors (e.g. key not found and script doesn't handle it gracefully by returning nil)
            // This could be treated as path not existing, or an actual error.
            // For now, let's assume script errors are more severe than "not found".
            // A robust Lua script should return nil if key/path not found.
            // If it throws (e.g. syntax error in path for some Lua parsers), then it's an error.
            // Let's assume LuaScriptException means a more fundamental issue here.
            // For robustness, one might catch specific Lua errors that mean "not found" and return false.
            throw RedisCommandException("LUA_json_path_type", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
    // Fallback to satisfy compiler, should be unreachable due to boolean _is_swss_mode
    throw RedisJSONException("Internal logic error: exists_path reached end of function unexpectedly for key '" + key + "', path '" + path_str + "'.");
}


// --- Array Operations (Client-Side for SWSS) ---
// These will follow the get-modify-set pattern using _json_modifier

void RedisJSONClient::append_path(const std::string& key, const std::string& path_str, const json& value) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = _get_document_for_modification(key);
        try {
            _json_modifier->array_append(doc, _path_parser->parse(path_str), value); // create_path is implicit in how array_append in modifier might work
            _set_document_after_modification(key, doc, opts);
        } catch (const TypeMismatchException& e){
            throw;
        } catch (const PathNotFoundException& e){ // If path doesn't exist and create_path is false
            throw;
        } catch (const json::exception& e) {
            throw RedisCommandException("APPEND_PATH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, value.dump()};
        try {
            _lua_script_manager->execute_script("json_array_append", keys, args);
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_array_append", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}

void RedisJSONClient::prepend_path(const std::string& key, const std::string& path_str, const json& value) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = _get_document_for_modification(key);
        try {
            _json_modifier->array_prepend(doc, _path_parser->parse(path_str), value);  // create_path is implicit
            _set_document_after_modification(key, doc, opts);
        } catch (const TypeMismatchException& e){
            throw;
        } catch (const PathNotFoundException& e){
            throw;
        } catch (const json::exception& e) {
            throw RedisCommandException("PREPEND_PATH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, value.dump()};
        try {
            _lua_script_manager->execute_script("json_array_prepend", keys, args);
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_array_prepend", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}

json RedisJSONClient::pop_path(const std::string& key, const std::string& path_str, int index) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = get_json(key); // pop requires path to exist
        json popped_value;
        try {
            popped_value = _json_modifier->array_pop(doc, _path_parser->parse(path_str), index);
            _set_document_after_modification(key, doc, opts);
            return popped_value;
        } catch (const TypeMismatchException& e){ // Path not an array
            throw;
        } catch (const PathNotFoundException& e){ // Path (or key) does not exist
            throw;
        } catch (const json::out_of_range& e) { // Index out of bounds from nlohmann::json if modifier doesn't catch it
            throw PathNotFoundException(key, path_str + "[" + std::to_string(index) + "] (Index out of bounds for pop: " + std::string(e.what()) + ")");
        } catch (const IndexOutOfBoundsException& e) { // Custom exception from modifier
             throw PathNotFoundException(key, path_str + "[" + std::to_string(index) + "] (Index out of bounds for pop: " + std::string(e.what()) + ")");
        } catch (const json::exception& e) { // Other json errors
            throw RedisCommandException("POP_PATH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, std::to_string(index)};
        try {
            json result = _lua_script_manager->execute_script("json_array_pop", keys, args);
            if (result.is_null()) throw PathNotFoundException(key, path_str); // Lua script returns nil if path not found/array empty/index OOB
            return result;
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_array_pop", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
    // Fallback to satisfy compiler, should be unreachable due to boolean _is_swss_mode
    throw RedisJSONException("Internal logic error: pop_path reached end of function unexpectedly for key '" + key + "', path '" + path_str + "'.");
}

size_t RedisJSONClient::array_length(const std::string& key, const std::string& path_str) const {
    if (_is_swss_mode) {
        json doc = get_json(key); // Throws if key not found
        try {
            auto parsed_path = _path_parser->parse(path_str);
            json::value_t type = _json_modifier->get_type(doc, parsed_path);
            if (type != json::value_t::array) {
                // Create a temporary json object of the actual type to get its name
                throw TypeMismatchException(path_str, "array", json(type).type_name());
            }
            return _json_modifier->get_size(doc, parsed_path);
        } catch (const PathNotFoundException& e){ // Path does not exist within doc or key itself not found
            throw; // Rethrow original PathNotFoundException from get_json or get_type/get_size
        } catch (const TypeMismatchException& e){ // Path not an array
            throw;
        } catch (const json::exception& e) { // Other json errors from modifier
            throw RedisCommandException("ARRAY_LENGTH (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str};
        try {
            json result = _lua_script_manager->execute_script("json_array_length", keys, args);
            if (result.is_number_integer()) {
                long long len = result.get<long long>();
                if (len < 0) throw RedisCommandException("LUA_json_array_length", "Negative length received");
                return static_cast<size_t>(len);
            }
            if (result.is_null()) throw PathNotFoundException(key, path_str); // Not an array or path not found
            throw RedisCommandException("LUA_json_array_length", "Unexpected result type: " + result.dump());
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_array_length", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}

long long RedisJSONClient::arrinsert(const std::string& key, const std::string& path_str, int index, const std::vector<json>& values) {
    if (values.empty()) {
        throw ArgumentInvalidException("Values vector cannot be empty for arrinsert.");
    }
    if (_is_swss_mode) {
        // Client-side get-modify-set
        SetOptions opts;
        json doc = _get_document_for_modification(key); // Creates {} if key not found
        try {
            // _json_modifier would need an arrinsert method
            // For now, manually implement the logic or assume _json_modifier has it.
            // This is a placeholder, actual implementation would involve more complex logic in JSONModifier
            // or here directly using nlohmann::json capabilities.
            // _json_modifier->array_insert(doc, _path_parser->parse(path_str), index, values);
            // Let's simulate a simplified version of this logic here for now if modifier is not ready
            json* target_array = nullptr;
            auto parsed_path_elements = _path_parser->parse(path_str);

            json* current = &doc;
            bool path_is_root = (path_str == "$" || path_str == "" || parsed_path_elements.empty());

            if (!path_is_root) {
                for (size_t i = 0; i < parsed_path_elements.size(); ++i) {
                    const auto& element = parsed_path_elements[i];
                    if (element.type == PathParser::PathElement::Type::KEY) {
                        if (!current->is_object()) throw PathNotFoundException(key, path_str); // Intermediate path not an object
                        // Ensure key exists before dereferencing, or handle creation if necessary.
                        // For arrinsert, path must exist up to the array.
                        if (!current->contains(element.key_name)) throw PathNotFoundException(key, path_str);
                        current = &(*current)[element.key_name];
                    } else if (element.type == PathParser::PathElement::Type::INDEX) {
                        if (!current->is_array()) throw PathNotFoundException(key, path_str); // Intermediate path not an array
                        if (element.index < 0 || static_cast<size_t>(element.index) >= current->size()) throw PathNotFoundException(key, path_str); // Index out of bounds
                        current = &(*current)[element.index];
                    } else {
                        // Other path element types (SLICE, WILDCARD etc.) are not straightforward for direct modification path.
                        // This simplified client-side arrinsert might only support KEY/INDEX paths.
                        throw InvalidPathException("SWSS arrinsert currently only supports KEY and INDEX path elements. Path: " + path_str);
                    }
                    if (current->is_null() && i < parsed_path_elements.size() -1) { // Intermediate path element is null
                        throw PathNotFoundException(key, path_str);
                    }
                }
            }
            target_array = current;


            if (!target_array->is_array()) {
                 // If path is root and root is not an array, and we are trying to insert,
                 // this should be an error unless the root is empty and becomes an array.
                 // For simplicity, if not an array, throw.
                if (target_array->is_null() && path_is_root) { // Key did not exist, root path
                    *target_array = json::array(); // Initialize as empty array
                } else if (target_array->is_null()) { // Path resolved to null, try to make it an array
                     // This implies create_path=true. If path was "a.b" and "a" existed but "b" was null
                     // We would need to make "b" an array. This is complex.
                     // For arrinsert, the array must exist.
                    throw TypeMismatchException(path_str, "array", "null");
                }
                else {
                    throw TypeMismatchException(path_str, "array", target_array->type_name());
                }
            }

            // nlohmann::json array iterators are complex for insert.
            // Easier to convert to std::vector, insert, then convert back, or use direct element access + erase/insert.
            // For simplicity and nlohmann specific:
            // Convert 0-based client index to iterator position
            auto it = target_array->begin();
            int arr_len = target_array->size();
            int insert_pos = index;

            if (insert_pos < 0) { // Negative index
                insert_pos = arr_len + insert_pos;
            }
            // Clamp insert_pos to [0, arr_len]
            if (insert_pos < 0) insert_pos = 0;
            if (insert_pos > arr_len) insert_pos = arr_len;

            std::advance(it, insert_pos);

            // Insert elements one by one
            for (const auto& val_to_insert : values) {
                it = target_array->insert(it, val_to_insert); // insert returns iterator to inserted element
                std::advance(it, 1); // Advance iterator to insert next element after the current one
            }

            _set_document_after_modification(key, doc, opts);
            return target_array->size();
        } catch (const TypeMismatchException& e){
            throw;
        } catch (const PathNotFoundException& e){
            throw;
        } catch (const json::exception& e) {
            throw RedisCommandException("ARRINSERT (SWSS-Client)", "Key: " + key + ", Path: " + path_str + ", JSON mod error: " + e.what());
        }
    } else { // Legacy Lua script
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");

        std::vector<std::string> script_args;
        script_args.push_back(path_str);
        script_args.push_back(std::to_string(index));
        for(const auto& val : values) {
            script_args.push_back(val.dump());
        }

        try {
            json result = _lua_script_manager->execute_script("json_array_insert", {key}, script_args);
            if (result.is_number_integer()) {
                return result.get<long long>();
            }
            throw RedisCommandException("LUA_json_array_insert", "Unexpected result type: " + result.dump());
        } catch (const LuaScriptException& e) {
            throw RedisCommandException("LUA_json_array_insert", "Key: " + key + ", Path: " + path_str + ", Error: " + e.what());
        }
    }
}


// --- Merge Operations ---
// Simplified merge_json for SWSS mode to be client-side.
// The original merge_json used JSON.MERGE which is specific to RedisJSON module.
// SWSS mode will do a client-side deep merge.
void RedisJSONClient::merge_json(const std::string& key, const json& patch) {
    if (_is_swss_mode) {
        SetOptions opts; // Default set options
        json current_doc;
        try {
            current_doc = get_json(key);
        } catch (const PathNotFoundException&) {
            // If key doesn't exist, merging into it means the patch becomes the new document,
            // but only if patch is an object or array. Otherwise, it's not a valid merge target.
            if (patch.is_object() || patch.is_array()) {
                current_doc = patch.is_object() ? json::object() : json::array(); // Start with compatible empty type
            } else {
                // Cannot merge a primitive into a non-existent key to make it the root.
                // Or, treat as set_json(key, patch)? For merge, usually expects containers.
                // Let's be strict: if key not found and patch isn't object/array, this is an error or no-op.
                // For simplicity, if key not found, the patch becomes the new document.
                set_json(key, patch, opts);
                return;
            }
        }

        try {
            // Perform a client-side merge (nlohmann::json::merge_patch or manual deep merge)
            // nlohmann::json::update is like a shallow merge (top-level keys from patch override current_doc)
            // nlohmann::json::merge_patch applies JSON Merge Patch (RFC 7386) semantics.
            // The original JSON.MERGE is more like a deep recursive update.
            // We will use `current_doc.merge_patch(patch);`
            current_doc.merge_patch(patch); // This performs RFC 7386 merge.
                                          // For a deep recursive update like ReJSON's JSON.MERGE,
                                          // a custom recursive function would be needed or use `current_doc.update(patch, true);` if available and suitable.
                                          // nlohmann `update` with `recursive=true` might be closer.
                                          // json::update(const_reference patch, bool_recursive)
                                          // Let's try `update` as it's more common for deep merging.
                                          // current_doc.update(patch, true); // This is not a standard nlohmann method.
                                          // `merge_patch` is standard. Let's stick to it and document difference.
            _set_document_after_modification(key, current_doc, opts);
        } catch (const json::exception& e) {
            throw RedisCommandException("MERGE_JSON (SWSS-Client)", "Key: " + key + ", JSON mod error: " + e.what());
        }

    } else { // Legacy mode (uses JSON.MERGE)
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        std::string patch_str = patch.dump();
        // Assuming MergeStrategy::DEEP is the only one effectively used, matching JSON.MERGE default.
        RedisReplyPtr reply(static_cast<redisReply*>(
            conn->command("JSON.MERGE %s $ %s", key.c_str(), patch_str.c_str())
        ));

        if (!reply) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: No reply or connection error");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: " + (reply->str ? reply->str : "Redis error"));
        }
        // Check for "OK" status (ReJSON 2.0+)
        if (!(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0)) {
             // Older ReJSON might return integer. For simplicity, expect OK for modern RedisJSON.
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Unexpected reply: " + (reply->str ? reply->str : "Non-OK status"));
        }
        _connection_manager->return_connection(std::move(conn));
    }
}


void RedisJSONClient::patch_json(const std::string& key, const json& patch_operations) {
    // This is client-side for both modes currently, as Redis doesn't have a standard JSON.PATCH.
    // The atomicity is lost.
    json current_doc;
    SetOptions opts; // Default set options
    try {
        current_doc = get_json(key);
    } catch (const PathNotFoundException& ) {
        // If key doesn't exist, applying a patch might be valid if it's an "add" to root.
        // nlohmann::json::patch expects a document. Start with null or empty object.
        // RFC 6902: "If the target document is an empty JSON document..."
        // Let's start with null, nlohmann's patch can handle add to root of null.
        current_doc = json(nullptr);
    }

    try {
        json patched_doc = current_doc.patch(patch_operations); // nlohmann::json::patch
        set_json(key, patched_doc, opts); // Use the class's set_json
    } catch (const json::exception& e) { // from patch operation
        throw PatchFailedException("Failed to apply JSON Patch for key '" + key + "': " + e.what());
    }
    // Exceptions from set_json are already handled by it.
}

// --- Atomic Operations (Non-Atomic for SWSS) ---

json RedisJSONClient::non_atomic_get_set(const std::string& key, const std::string& path_str,
                                         const json& new_value) {
    if (_is_swss_mode) {
        // Non-atomic: Get full doc, get path, set path, set full doc
        json doc = _get_document_for_modification(key); // Creates empty {} if key not found
        json old_value_at_path = json(nullptr); // Default if path doesn't exist
        try {
            old_value_at_path = _json_modifier->get(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            // Path doesn't exist, old_value_at_path remains null. This is fine.
        }

        SetOptions opts; // Default set options
        // opts.create_path should be true for get_set to ensure the path is created.
        _json_modifier->set(doc, _path_parser->parse(path_str), new_value, true /*create_path*/, true /*overwrite*/);
        _set_document_after_modification(key, doc, opts);
        return old_value_at_path;

    } else { // Legacy mode (atomic via Lua)
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        const std::string script_name = "json_get_set";
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, new_value.dump()};
        try {
            return _lua_script_manager->execute_script(script_name, keys, args);
        } catch (const LuaScriptException& e) {
            throw LuaScriptException(script_name, "Key '" + key + "', path '" + path_str + "': " + e.what());
        }
    }
}

bool RedisJSONClient::non_atomic_compare_set(const std::string& key, const std::string& path_str,
                                            const json& expected_val, const json& new_val) {
    if (_is_swss_mode) {
        // Non-atomic: Get full doc, get path, compare, if match then set path, set full doc
        json doc;
        try {
            doc = get_json(key);
        } catch (const PathNotFoundException&) {
            // Key doesn't exist. If expected is null and path is simple (e.g. root or direct child),
            // this might be a valid CAS to create. For now, if key not found, CAS fails.
            return false;
        }

        json current_val_at_path = json(nullptr);
        bool path_existed = true;
        try {
            current_val_at_path = _json_modifier->get(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            path_existed = false; // current_val_at_path remains null
        }

        if (path_existed && current_val_at_path == expected_val) {
            SetOptions opts; // Default set options
            _json_modifier->set(doc, _path_parser->parse(path_str), new_val, true /*create_path*/, true /*overwrite*/);
            _set_document_after_modification(key, doc, opts);
            return true;
        } else if (!path_existed && expected_val.is_null()) { // Path didn't exist, and we expected null (i.e. path not to exist)
            SetOptions opts;
            _json_modifier->set(doc, _path_parser->parse(path_str), new_val, true /*create_path*/, true /*overwrite*/);
            _set_document_after_modification(key, doc, opts);
            return true;
        }
        return false;

    } else { // Legacy
        if (!_lua_script_manager) throw RedisJSONException("LuaScriptManager not initialized.");
        const std::string script_name = "json_compare_set";
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {path_str, expected_val.dump(), new_val.dump()};
        try {
            json result_json = _lua_script_manager->execute_script(script_name, keys, args);
            if (result_json.is_number_integer()) return result_json.get<int>() == 1;
            throw LuaScriptException(script_name, "Non-integer result: " + result_json.dump());
        } catch (const LuaScriptException& e) {
            throw LuaScriptException(script_name, "Key '" + key + "', path '" + path_str + "': " + e.what());
        }
    }
}

bool RedisJSONClient::set_json_sparse(const std::string& key, const json& sparse_json_object) {
    if (_is_swss_mode) {
        // For SWSS mode, this would be a client-side get-merge-set.
        // The request is to implement this for non-SWSS mode using Lua.
        throw NotImplementedException("set_json_sparse for SWSS mode is not implemented with atomicity. Use non-SWSS mode for Lua script execution.");
    } else {
        if (!_lua_script_manager) {
            throw RedisJSONException("LuaScriptManager not initialized for set_json_sparse.");
        }
        if (!sparse_json_object.is_object()) {
            // This check ensures that we are trying to merge an object.
            // The Lua script also validates this, but client-side check is good.
            throw ArgumentInvalidException("Input sparse_json_object must be a JSON object for set_json_sparse.");
        }
        try {
            std::string sparse_json_str = sparse_json_object.dump();
            json result = _lua_script_manager->execute_script(
                "json_sparse_merge", // Script name defined in LuaScriptManager
                {key},               // KEYS[1]
                {sparse_json_str}    // ARGV[1]
            );

            // The Lua script `json_sparse_merge` is designed to return:
            // - 1 on success.
            // - Error reply (which LuaScriptManager converts to LuaScriptException) on failure.
            if (result.is_number() && result.get<int>() == 1) {
                return true;
            } else {
                // This case should ideally not be reached if the Lua script strictly returns 1 or throws an error.
                // If it's reached, it means the script returned something unexpected without erroring in Redis.
                // Example: Lua script returns `false` or `nil` directly instead of `redis.error_reply()`.
                // We'll treat such cases as a general failure of the operation.
                throw RedisJSONException("Lua script 'json_sparse_merge' for key '" + key + "' returned an unexpected result: " + result.dump());
            }
        } catch (const LuaScriptException& e) {
            // Re-throw to propagate Lua script errors (e.g., ERR_DECODE_ARG, ERR_EXISTING_TYPE)
            throw;
        } catch (const JsonParsingException& e) {
            // Error during sparse_json_object.dump()
            throw; // Re-throw, already specific enough
        } catch (const RedisJSONException& e) {
            // Catch other potential RedisJSON exceptions from execute_script (e.g., connection issues)
            throw RedisJSONException("Error during set_json_sparse for key '" + key + "': " + e.what());
        }
        // Fallback, though ideally try/catch should handle all returns/throws.
        // This addresses the -Wreturn-type warning if any path through try/catch isn't covered.
        // However, the logic above (throwing or returning true) should cover all paths from `try`.
        // If an unexpected std::exception (not RedisJSONException) occurs, it would propagate.
    }
    // This line should ideally be unreachable if the logic within the else block is complete.
    // If _is_swss_mode is false, the code must enter the else block.
    // The else block should either return true or throw an exception.
    // Adding a "return false;" here to satisfy the compiler if it can't deduce full coverage,
    // but it indicates a potential logic flaw if ever reached.
    return false;
}

std::vector<std::string> RedisJSONClient::object_keys(const std::string& key, const std::string& path) {
    if (_is_swss_mode) {
        // Client-side implementation for SWSS mode:
        // 1. Get the full JSON document.
        // 2. Navigate to the path.
        // 3. If it's an object, extract keys.
        // This is non-atomic.
        json doc;
        try {
            doc = get_json(key); // Throws PathNotFoundException if key doesn't exist
        } catch (const PathNotFoundException&) {
            return {}; // Key not found, so no object keys
        }

        json target_node = doc;
        if (path != "$" && path != "" && path != ".") { // PathParser might normalize "" to "$"
            try {
                // Use the existing get_path logic to extract the node at the path
                // This relies on _json_modifier and _path_parser being correctly set up
                // and get_path correctly extracting the sub-document.
                target_node = _json_modifier->get(doc, _path_parser->parse(path));
            } catch (const PathNotFoundException&) {
                return {}; // Path not found within the document
            } catch (const json::exception& e) { // Catch errors from nlohmann during path traversal
                throw InvalidPathException("Error accessing path '" + path + "' in key '" + key + "' for object_keys (SWSS): " + e.what());
            }
        }

        if (target_node.is_object()) {
            std::vector<std::string> keys_vec;
            for (auto it = target_node.begin(); it != target_node.end(); ++it) {
                keys_vec.push_back(it.key());
            }
            return keys_vec;
        } else {
            // Path does not point to an object, or path was root and root is not an object
            return {};
        }
    } else { // Legacy mode (Lua script)
        if (!_lua_script_manager) {
            throw RedisJSONException("LuaScriptManager not initialized for object_keys.");
        }

        try {
            json result = _lua_script_manager->execute_script("json_object_keys", {key}, {path});

            if (result.is_null()) { // Lua script returns nil (json null) for non-object/path-not-found/key-not-found
                return {};
            }
            if (result.is_array()) {
                std::vector<std::string> keys_vec;
                for (const auto& item : result) {
                    if (item.is_string()) {
                        keys_vec.push_back(item.get<std::string>());
                    } else {
                        throw JsonParsingException("Lua script json_object_keys returned non-string element in array for key '" + key + "', path '" + path + "'");
                    }
                }
                return keys_vec;
            }
            // Should be an array or null. If something else, it's unexpected.
            throw RedisCommandException("json_object_keys", "Unexpected reply format from Lua script for key '" + key + "', path '" + path + "'. Expected array or null, got: " + result.dump());
        } catch (const LuaScriptException& e) {
            // Check if the error message indicates a path not found or type error that should result in empty list
            // This depends on how strictly the Lua script reports errors vs. returns nil for "not found" conditions.
            // The current Lua script is designed to return nil for "not found" or "not an object".
            // So, a LuaScriptException here is likely a more fundamental script error or Redis error.
            throw; // Re-throw LuaScriptException
        }
    }
}

// --- Utility Operations ---

std::vector<std::string> RedisJSONClient::keys_by_pattern(const std::string& pattern) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        try {
            // Assuming DBConnector has a keys method that takes a pattern.
            // Standard Redis KEYS command is blocking. SCAN is preferred.
            // If DBConnector::keys uses SCAN, great. Otherwise, this might be an issue.
            // For now, assume it's available and reasonably performant.
            return _db_connector->keys(pattern);
        } catch (const std::exception& e) {
            throw RedisCommandException("KEYS (SWSS)", "Pattern: " + pattern + ", Error: " + e.what());
        }
    } else { // Legacy
        std::vector<std::string> found_keys;
        std::string cursor = "0";
        RedisConnectionManager::RedisConnectionPtr conn;

        do {
            conn = get_legacy_redis_connection();
            RedisReplyPtr reply(static_cast<redisReply*>(
                conn->command("SCAN %s MATCH %s COUNT 100", cursor.c_str(), pattern.c_str())
            ));

            if (!reply) {
                if(conn) _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("SCAN", "Pattern: " + pattern + ", No reply");
            }
            if (reply->type == REDIS_REPLY_ERROR) {
                 std::string err = reply->str ? reply->str : "Unknown SCAN error";
                if(conn) _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: " + err);
            }
            if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
                if(conn) _connection_manager->return_connection(std::move(conn));
                throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Unexpected reply structure");
            }

            redisReply* cursor_reply = reply->element[0];
            cursor = std::string(cursor_reply->str, cursor_reply->len);

            redisReply* keys_reply = reply->element[1];
            for (size_t i = 0; i < keys_reply->elements; ++i) {
                if (keys_reply->element[i]->type == REDIS_REPLY_STRING) {
                    found_keys.emplace_back(keys_reply->element[i]->str, keys_reply->element[i]->len);
                }
            }
            _connection_manager->return_connection(std::move(conn));
        } while (cursor != "0");
        return found_keys;
    }
}

// search_by_value and get_all_paths are client-side, so their core logic remains the same,
// they just use the (potentially mode-switched) get_json internally.

// Helper recursive function for search_by_value (copied from original, no changes needed)
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
}

json RedisJSONClient::search_by_value(const std::string& key, const json& search_value) const {
    json document_to_search;
    try {
        document_to_search = get_json(key); // Uses the mode-aware get_json
    } catch (const PathNotFoundException& ) {
        return json::array();
    }
    // No change to recursive search logic needed
    json::array_t results_array;
    find_values_recursive(document_to_search, search_value, results_array);
    return json(results_array);
}

// Helper recursive function for get_all_paths (copied from original, no changes needed)
void find_json_paths_recursive(const json& current_node, const std::string& current_path_str, std::vector<std::string>& paths_list) {
    if (current_node.is_object()) {
        if (current_node.empty() && current_path_str != "$") {
            // paths_list.push_back(current_path_str); // Optional: path to empty object itself
        }
        for (auto& el : current_node.items()) {
            // PathParser::escape_key_if_needed is the correct static method.
            std::string child_path = current_path_str + "." + PathParser::escape_key_if_needed(el.key());
            paths_list.push_back(child_path);
            find_json_paths_recursive(el.value(), child_path, paths_list);
        }
    } else if (current_node.is_array()) {
        if (current_node.empty() && current_path_str != "$") {
            // paths_list.push_back(current_path_str); // Optional: path to empty array itself
        }
        for (size_t i = 0; i < current_node.size(); ++i) {
            std::string child_path = current_path_str + "[" + std::to_string(i) + "]";
            paths_list.push_back(child_path);
            find_json_paths_recursive(current_node[i], child_path, paths_list);
        }
    }
}

std::vector<std::string> RedisJSONClient::get_all_paths(const std::string& key) const {
    json document;
    try {
        document = get_json(key); // Uses the mode-aware get_json
    } catch (const PathNotFoundException& ) {
        return {};
    }

    std::vector<std::string> all_paths;
    if (!document.is_null()) {
        all_paths.push_back("$");
        if (!document.empty() || document.is_object() || document.is_array()) { // only recurse if not primitive or null
             find_json_paths_recursive(document, "$", all_paths);
        }
    }
    return all_paths;
}


// --- Access to sub-components ---
// These are commented out in the header for SWSS mode.
// If needed, they would require significant adaptation.
/*
JSONQueryEngine& RedisJSONClient::query_engine() {
    // ...
}
// ... etc. for cache, schema_validator, event_emitter, transaction_manager
*/


// --- Internal Helpers ---
// _perform_write_operation and _get_document_with_caching were for legacy connection manager and complex features.
// They are likely not directly applicable or need complete rethink for SWSS mode.
// For now, they are effectively unused if client-side path ops call get_json/set_json directly.

json RedisJSONClient::_parse_json_reply(const std::string& reply_str, const std::string& context_msg) const {
    try {
        return json::parse(reply_str);
    } catch (const json::parse_error& e) {
        throw JsonParsingException(context_msg + ": " + e.what() + ". Received: " + reply_str);
    }
}

// ... other method implementations ...

long long RedisJSONClient::json_clear(const std::string& key, const std::string& path) {
    if (key.empty()) {
        throw ArgumentInvalidException("Key cannot be empty for JSON.CLEAR operation.");
    }
    // Path can be empty (which defaults to '$' in Lua script) or explicitly '$'.

    try {
        json result_json = _lua_script_manager->execute_script( // Fix 2: Typo
            "json_clear",
            {key},            // KEYS
            {path}            // ARGV
        );

        if (result_json.is_number_integer()) {
            return result_json.get<long long>();
        } else if (result_json.is_null()) {
            return 0;
        }
        else if (result_json.is_string()) {
             // This case should ideally be caught by LuaScriptException if the script itself returns redis.error_reply()
             // and redis_reply_to_json throws LuaScriptException for REDIS_REPLY_ERROR.
            throw RedisJSONException("JSON.CLEAR script returned an unexpected string: " + result_json.get<std::string>());
        }
        else {
            // Fix 3 & new fix for constructor: String concatenation and use single-arg constructor
            throw TypeMismatchException("JSON.CLEAR: Unexpected result type from Lua script: " + std::string(result_json.type_name()));
        }
    } catch (const LuaScriptException& e) {
        std::string what_str = e.what();
        if (what_str.find("ERR document not found") != std::string::npos) {
            // Fix 4 & new fix for constructor: Use two-arg constructor
            // The specific error message from Lua ("ERR document not found") will be in e.what()
            // The PathNotFoundException will then provide key and path context.
            throw PathNotFoundException(key, path);
        }
        throw;
    } catch (const RedisJSONException& e) {
        throw;
    } catch (const std::exception& e) {
        throw JsonParsingException("Failed to parse result for JSON.CLEAR: " + std::string(e.what()));
    }
    // Fix 5: Control reaches end of non-void function
    throw RedisJSONException("Reached end of non-void function json_clear unexpectedly");
}

} // namespace redisjson
