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
}

// Constructor for SONiC SWSS environment
RedisJSONClient::RedisJSONClient(const SwssClientConfig& swss_config)
    : _is_swss_mode(true), _swss_config(swss_config) {
    try {
        _db_connector = std::make_unique<swss::DBConnector>(
            _swss_config.db_name,
            _swss_config.operation_timeout_ms,
            _swss_config.wait_for_db,
            _swss_config.unix_socket_path
        );
    } catch (const std::exception& e) {
        throw ConnectionException("SWSS DBConnector failed to initialize for DB '" + _swss_config.db_name + "': " + e.what());
    }
    _path_parser = std::make_unique<PathParser>();
    _json_modifier = std::make_unique<JSONModifier>();
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

void RedisJSONClient::throwIfNotLegacyWithLua(const std::string& operation_name) const {
    if (_is_swss_mode) {
        throw NotImplementedException("Operation '" + operation_name + "' is not supported in SWSS mode with Lua-like atomicity.");
    }
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager not initialized for operation '" + operation_name + "'.");
    }
}

// --- Document Operations ---

void RedisJSONClient::set_json(const std::string& key, const json& document, const SetOptions& opts) {
    std::string doc_str = document.dump();
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        // Simplified SWSS SET handling
        _db_connector->set(key, doc_str);
        if (opts.ttl.count() > 0) {
            // TTL handling in SWSS might need specific DBConnector features or be unsupported
            // For now, this is a placeholder or might be ignored.
            // std::cerr << "Warning: TTL option for set_json in SWSS mode is not fully supported by basic DBConnector." << std::endl;
        }
        if (opts.condition != SetCmdCondition::NONE) {
            // NX/XX conditions are not directly supported by basic DBConnector->set
            // std::cerr << "Warning: NX/XX conditions for set_json in SWSS mode are not supported by basic DBConnector." << std::endl;
        }
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply;
        std::vector<const char*> argv_c;
        std::vector<size_t> argv_len;

        argv_c.push_back("SET");
        argv_c.push_back(key.c_str());
        argv_c.push_back(doc_str.c_str());

        std::string ttl_str_holder; // To keep std::string alive for c_str()

        if (opts.ttl.count() > 0) {
            argv_c.push_back("EX");
            ttl_str_holder = std::to_string(opts.ttl.count());
            argv_c.push_back(ttl_str_holder.c_str());
        }
        if (opts.condition == SetCmdCondition::NX) {
            argv_c.push_back("NX");
        } else if (opts.condition == SetCmdCondition::XX) {
            argv_c.push_back("XX");
        }

        for(const char* s : argv_c) {
            argv_len.push_back(strlen(s));
        }

        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command_argv(argv_c.size(), argv_c.data(), argv_len.data())
        ));

        if (!reply) {
             _connection_manager->return_connection(std::move(conn)); // Return connection before throwing
            throw RedisCommandException("SET", "Key: " + key + ", Error: No reply or connection error");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            std::string err_msg = reply->str ? reply->str : "Unknown Redis error";
            _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SET", "Key: " + key + ", Error: " + err_msg);
        }
        if (reply->type == REDIS_REPLY_NIL && opts.condition != SetCmdCondition::NONE) {
            // Condition (NX/XX) not met, this is not an error from Redis.
            // The operation simply didn't happen. Client might want to know this.
            // For now, we don't throw, implying success but no change.
            // Or, could return a bool from set_json indicating if set occurred.
        } else if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") != 0) {
            // Should not happen if not error and not nil for NX/XX
            std::string status_msg = reply->str ? reply->str : "Non-OK status";
             _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SET", "Key: " + key + ", SET command did not return OK: " + status_msg);
        }
        _connection_manager->return_connection(std::move(conn));
    }
}

json RedisJSONClient::get_json(const std::string& key) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        std::string doc_str = _db_connector->get(key);
        if (doc_str.empty()) {
            throw PathNotFoundException(key, "$ (root)");
        }
        return _parse_json_reply(doc_str, "SWSS GET for key '" + key + "'");
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("GET %s", key.c_str())));
        _connection_manager->return_connection(std::move(conn)); // Return connection after command

        if (!reply) {
            throw RedisCommandException("GET", "Key: " + key + ", Error: No reply or connection error");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            throw RedisCommandException("GET", "Key: " + key + ", Error: " + (reply->str ? reply->str : "Redis error reply"));
        }
        if (reply->type == REDIS_REPLY_NIL) {
            throw PathNotFoundException(key, "$ (root)");
        }
        if (reply->type == REDIS_REPLY_STRING) {
            return _parse_json_reply(reply->str, "GET for key '" + key + "'");
        }
        throw RedisCommandException("GET", "Key: " + key + ", Error: Unexpected reply type " + std::to_string(reply->type));
    }
}

bool RedisJSONClient::exists_json(const std::string& key) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        return _db_connector->exists(key);
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("EXISTS %s", key.c_str())));
        _connection_manager->return_connection(std::move(conn));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            throw RedisCommandException("EXISTS", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Unknown Redis error") : "No reply"));
        }
        return (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    }
}

void RedisJSONClient::del_json(const std::string& key) {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        _db_connector->del(key);
    } else { // Legacy mode
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("DEL %s", key.c_str())));
        _connection_manager->return_connection(std::move(conn));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            throw RedisCommandException("DEL", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Unknown Redis error") : "No reply"));
        }
    }
}

json RedisJSONClient::_get_document_for_modification(const std::string& key) const {
    try {
        return get_json(key);
    } catch (const PathNotFoundException& ) {
        return json::object();
    }
}

void RedisJSONClient::_set_document_after_modification(const std::string& key, const json& document, const SetOptions& opts) {
    set_json(key, document, opts);
}

// --- Path Operations ---
json RedisJSONClient::get_path(const std::string& key, const std::string& path_str) const {
    if (path_str == "$" || path_str == ".") {
        return get_json(key);
    }
    if (_is_swss_mode) {
        json current_doc = get_json(key);
        return _json_modifier->get(current_doc, _path_parser->parse(path_str));
    } else {
        throwIfNotLegacyWithLua("json_path_get");
        json result = _lua_script_manager->execute_script("json_path_get", {key}, {path_str});
        if (result.is_array() && result.empty()) throw PathNotFoundException(key, path_str);
            return result;
    }
}

void RedisJSONClient::set_path(const std::string& key, const std::string& path_str,
                               const json& value, const SetOptions& opts) {
    if (path_str == "$" || path_str == ".") {
        set_json(key, value, opts);
        return;
    }
    if (_is_swss_mode) {
        json doc = _get_document_for_modification(key);
        _json_modifier->set(doc, _path_parser->parse(path_str), value, opts.create_path);
        _set_document_after_modification(key, doc, opts);
    } else {
        throwIfNotLegacyWithLua("json_path_set");
        std::string value_dump = value.dump();
        std::string condition_str;
        switch (opts.condition) {
            case SetCmdCondition::NX: condition_str = "NX"; break;
            case SetCmdCondition::XX: condition_str = "XX"; break;
            default: condition_str = "NONE"; break;
        }
        _lua_script_manager->execute_script("json_path_set", {key}, {path_str, value_dump, condition_str, std::to_string(opts.ttl.count()), opts.create_path ? "true" : "false"});
    }
}

void RedisJSONClient::del_path(const std::string& key, const std::string& path_str) {
    if (path_str == "$" || path_str == ".") {
        del_json(key);
        return;
    }
     if (_is_swss_mode) {
        SetOptions opts;
        json doc;
        try {
            doc = get_json(key);
        } catch (const PathNotFoundException&) {
            return;
        }
        try {
            _json_modifier->del(doc, _path_parser->parse(path_str));
            _set_document_after_modification(key, doc, opts);
        } catch (const PathNotFoundException& ) {
            return;
        }
    } else {
        throwIfNotLegacyWithLua("json_path_del");
        _lua_script_manager->execute_script("json_path_del", {key}, {path_str});
    }
}

bool RedisJSONClient::exists_path(const std::string& key, const std::string& path_str) const {
    if (path_str == "$" || path_str == ".") {
        return exists_json(key);
    }
    if (_is_swss_mode) {
        try {
            json doc = get_json(key);
            return _json_modifier->exists(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            return false;
        }
    } else {
        throwIfNotLegacyWithLua("json_path_type");
        json result = _lua_script_manager->execute_script("json_path_type", {key}, {path_str});
        return !result.is_null();
    }
}

// --- Array Operations ---
void RedisJSONClient::append_path(const std::string& key, const std::string& path_str, const json& value) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = _get_document_for_modification(key);
        _json_modifier->array_append(doc, _path_parser->parse(path_str), value);
        _set_document_after_modification(key, doc, opts);
    } else {
        throwIfNotLegacyWithLua("json_array_append");
        _lua_script_manager->execute_script("json_array_append", {key}, {path_str, value.dump()});
    }
}

void RedisJSONClient::prepend_path(const std::string& key, const std::string& path_str, const json& value) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = _get_document_for_modification(key);
        _json_modifier->array_prepend(doc, _path_parser->parse(path_str), value);
        _set_document_after_modification(key, doc, opts);
    } else {
        throwIfNotLegacyWithLua("json_array_prepend");
        _lua_script_manager->execute_script("json_array_prepend", {key}, {path_str, value.dump()});
    }
}

json RedisJSONClient::pop_path(const std::string& key, const std::string& path_str, int index) {
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = get_json(key);
        json popped_value;
        popped_value = _json_modifier->array_pop(doc, _path_parser->parse(path_str), index);
        _set_document_after_modification(key, doc, opts);
        return popped_value;
    } else {
        throwIfNotLegacyWithLua("json_array_pop");
        json result = _lua_script_manager->execute_script("json_array_pop", {key}, {path_str, std::to_string(index)});
        if (result.is_null()) throw PathNotFoundException(key, path_str);
        return result;
    }
}

size_t RedisJSONClient::array_length(const std::string& key, const std::string& path_str) const {
    if (_is_swss_mode) {
        json doc = get_json(key);
        auto parsed_path = _path_parser->parse(path_str);
        json::value_t type = _json_modifier->get_type(doc, parsed_path);
        if (type != json::value_t::array) {
            throw TypeMismatchException(path_str, "array", json(type).type_name());
        }
        return _json_modifier->get_size(doc, parsed_path);
    } else {
        throwIfNotLegacyWithLua("json_array_length");
        json result = _lua_script_manager->execute_script("json_array_length", {key}, {path_str});
        if (result.is_number_integer()) {
            long long len = result.get<long long>();
            if (len < 0) throw RedisCommandException("LUA_json_array_length", "Negative length received");
            return static_cast<size_t>(len);
        }
        if (result.is_null()) throw PathNotFoundException(key, path_str);
        throw RedisCommandException("LUA_json_array_length", "Unexpected result type: " + result.dump());
    }
}

long long RedisJSONClient::arrinsert(const std::string& key, const std::string& path_str, int index, const std::vector<json>& values) {
    if (values.empty()) {
        throw ArgumentInvalidException("Values vector cannot be empty for arrinsert.");
    }
    if (_is_swss_mode) {
        SetOptions opts;
        json doc = _get_document_for_modification(key);
        json* target_array = nullptr;
        auto parsed_path_elements = _path_parser->parse(path_str);
        json* current = &doc;
        bool path_is_root = (path_str == "$" || path_str == "" || parsed_path_elements.empty());

        if (!path_is_root) {
            for (size_t i = 0; i < parsed_path_elements.size(); ++i) {
                const auto& element = parsed_path_elements[i];
                if (element.type == PathParser::PathElement::Type::KEY) {
                    if (!current->is_object()) throw PathNotFoundException(key, path_str);
                    if (!current->contains(element.key_name)) throw PathNotFoundException(key, path_str);
                    current = &(*current)[element.key_name];
                } else if (element.type == PathParser::PathElement::Type::INDEX) {
                    if (!current->is_array()) throw PathNotFoundException(key, path_str);
                    if (element.index < 0 || static_cast<size_t>(element.index) >= current->size()) throw PathNotFoundException(key, path_str);
                    current = &(*current)[element.index];
                } else {
                    throw InvalidPathException("SWSS arrinsert currently only supports KEY and INDEX path elements. Path: " + path_str);
                }
                if (current->is_null() && i < parsed_path_elements.size() -1) {
                    throw PathNotFoundException(key, path_str);
                }
            }
        }
        target_array = current;

        if (!target_array->is_array()) {
            if (target_array->is_null() && path_is_root) {
                *target_array = json::array();
            } else if (target_array->is_null()) {
                throw TypeMismatchException(path_str, "array", "null");
            }
            else {
                throw TypeMismatchException(path_str, "array", target_array->type_name());
            }
        }
        auto it = target_array->begin();
        int arr_len = target_array->size();
        int insert_pos = index;
        if (insert_pos < 0) {
            insert_pos = arr_len + insert_pos;
        }
        if (insert_pos < 0) insert_pos = 0;
        if (insert_pos > arr_len) insert_pos = arr_len;
        std::advance(it, insert_pos);
        for (const auto& val_to_insert : values) {
            it = target_array->insert(it, val_to_insert);
            std::advance(it, 1);
        }
        _set_document_after_modification(key, doc, opts);
        return target_array->size();
    } else {
        throwIfNotLegacyWithLua("json_array_insert");
        std::vector<std::string> script_args;
        script_args.push_back(path_str);
        script_args.push_back(std::to_string(index));
        for(const auto& val : values) {
            script_args.push_back(val.dump());
        }
        json result = _lua_script_manager->execute_script("json_array_insert", {key}, script_args);
        if (result.is_number_integer()) {
            return result.get<long long>();
        }
        throw RedisCommandException("LUA_json_array_insert", "Unexpected result type: " + result.dump());
    }
}

long long RedisJSONClient::arrindex(const std::string& key,
                                    const std::string& path,
                                    const json& value_to_find,
                                    std::optional<long long> start_index,
                                    std::optional<long long> end_index) {
    if (_is_swss_mode) {
        json doc = get_json(key);
        json target_array_node;
        try {
            target_array_node = _json_modifier->get(doc, _path_parser->parse(path));
        } catch (const PathNotFoundException&) {
            throw PathNotFoundException(key, path); // Corrected
        } catch (const std::exception& e) {
             throw InvalidPathException("Error accessing path '" + path + "' for ARRINDEX: " + e.what());
        }

        if (!target_array_node.is_array()) {
            throw TypeMismatchException(path, "array", target_array_node.type_name()); // Corrected
        }
        if (!value_to_find.is_primitive() && !value_to_find.is_null()) {
             throw TypeMismatchException(path, "scalar", value_to_find.type_name()); // Corrected
        }

        long long current_start_idx = 0;
        if (start_index.has_value()) {
            current_start_idx = start_index.value();
            if (current_start_idx < 0) {
                current_start_idx = static_cast<long long>(target_array_node.size()) + current_start_idx;
            }
        }
        if (current_start_idx < 0) current_start_idx = 0;

        long long current_end_idx = static_cast<long long>(target_array_node.size()) - 1;
        if (end_index.has_value()) {
            current_end_idx = end_index.value();
             if (current_end_idx < 0) {
                current_end_idx = static_cast<long long>(target_array_node.size()) + current_end_idx;
            }
        }
        if (current_end_idx >= static_cast<long long>(target_array_node.size())) {
             current_end_idx = static_cast<long long>(target_array_node.size()) - 1;
        }

        if (current_start_idx > current_end_idx || target_array_node.empty() || current_start_idx >= static_cast<long long>(target_array_node.size()) ) {
            return -1;
        }

        for (long long i = current_start_idx; i <= current_end_idx; ++i) {
            if (target_array_node.at(static_cast<size_t>(i)) == value_to_find) {
                return i;
            }
        }
        return -1;
    }

    throwIfNotLegacyWithLua("arrindex");
    std::string value_json_str = value_to_find.dump();
    std::string start_str = start_index.has_value() ? std::to_string(start_index.value()) : "";
    std::string end_str = end_index.has_value() ? std::to_string(end_index.value()) : "";

    json script_result = _lua_script_manager->execute_script(
        "json_arrindex", {key}, {path, value_json_str, start_str, end_str}
    );

    if (script_result.is_number_integer()) {
        return script_result.get<long long>();
    }
    throw JsonParsingException("JSON.ARRINDEX script did not return an integer as expected. Got: " + script_result.dump());
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

        // Path must exist and be a number. RedisJSON errors if path doesn't exist for NUMINCRBY.
        if (!path_existed) {
            throw PathNotFoundException(key, path);
        }
        if (!current_value_at_path.is_number()) {
            throw TypeMismatchException(path, "number", current_value_at_path.type_name());
        }

        double new_numeric_value = current_value_at_path.get<double>() + value;
        json new_json_value = new_numeric_value;

        SetOptions opts;
        _json_modifier->set(doc, parsed_path, new_json_value, false /*create_path*/, true /*overwrite*/);
        _set_document_after_modification(key, doc, opts);
        return new_json_value;

    } else { // Legacy mode (atomic via Lua)
        throwIfNotLegacyWithLua("json_numincrby");
        std::string value_str = json(value).dump(); // Ensure double is correctly stringified for Lua
        json result = _lua_script_manager->execute_script("json_numincrby", {key}, {path, value_str});
        // Lua script for numincrby returns the new value, JSON encoded.
        // execute_script already parses this.
        return result;
    }
}

// --- Merge Operations ---
void RedisJSONClient::merge_json(const std::string& key, const json& patch) {
    if (_is_swss_mode) {
        SetOptions opts;
        json current_doc;
        try {
            current_doc = get_json(key);
        } catch (const PathNotFoundException&) {
            if (patch.is_object() || patch.is_array()) {
                current_doc = patch.is_object() ? json::object() : json::array();
            } else {
                set_json(key, patch, opts);
                return;
            }
        }
        current_doc.merge_patch(patch);
        _set_document_after_modification(key, current_doc, opts);
    } else {
        throwIfNotLegacyWithLua("JSON.MERGE"); // Assuming direct command for now
        RedisConnectionManager::RedisConnectionPtr conn = get_legacy_redis_connection();
        std::string patch_str = patch.dump();
        RedisReplyPtr reply(static_cast<redisReply*>(
            conn->command("JSON.MERGE %s $ %s", key.c_str(), patch_str.c_str())
        ));
        _connection_manager->return_connection(std::move(conn));
        if (!reply) {
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: No reply or connection error");
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: " + (reply->str ? reply->str : "Redis error"));
        }
        if (!(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0)) {
            throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Unexpected reply: " + (reply->str ? reply->str : "Non-OK status"));
        }
    }
}

bool RedisJSONClient::set_json_sparse(const std::string& key, const json& sparse_json_object) {
    if (_is_swss_mode) {
        throw NotImplementedException("set_json_sparse for SWSS mode is not implemented with atomicity. Use non-SWSS mode for Lua script execution.");
    } else {
        throwIfNotLegacyWithLua("json_sparse_merge");
        if (!sparse_json_object.is_object()) {
            throw ArgumentInvalidException("Input sparse_json_object must be a JSON object for set_json_sparse.");
        }
        std::string sparse_json_str = sparse_json_object.dump();
        json result = _lua_script_manager->execute_script("json_sparse_merge", {key}, {sparse_json_str});
        if (result.is_number() && result.get<int>() == 1) {
            return true;
        } else {
            throw RedisJSONException("Lua script 'json_sparse_merge' for key '" + key + "' returned an unexpected result: " + result.dump());
        }
    }
}

std::vector<std::string> RedisJSONClient::object_keys(const std::string& key, const std::string& path) {
    if (_is_swss_mode) {
        json doc;
        try {
            doc = get_json(key);
        } catch (const PathNotFoundException&) {
            return {};
        }
        json target_node = doc;
        if (path != "$" && path != "" && path != ".") {
            try {
                target_node = _json_modifier->get(doc, _path_parser->parse(path));
            } catch (const PathNotFoundException&) {
                return {};
            } catch (const json::exception& e) {
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
            return {};
        }
    } else {
        throwIfNotLegacyWithLua("json_object_keys");
        json result = _lua_script_manager->execute_script("json_object_keys", {key}, {path});
        if (result.is_null()) {
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
        throw RedisCommandException("json_object_keys", "Unexpected reply format from Lua script for key '" + key + "', path '" + path + "'. Expected array or null, got: " + result.dump());
    }
}

std::optional<size_t> RedisJSONClient::object_length(const std::string& key, const std::string& path) {
    if (_is_swss_mode) {
        json doc;
        try {
            doc = get_json(key);
        } catch (const PathNotFoundException&) {
            return std::nullopt;
        }
        json target_node = doc;
        if (path != "$" && path != "" && path != ".") {
            try {
                target_node = _json_modifier->get(doc, _path_parser->parse(path));
            } catch (const PathNotFoundException&) {
                return std::nullopt;
            } catch (const json::exception& e) {
                throw InvalidPathException("Error accessing path '" + path + "' in key '" + key + "' for object_length (SWSS): " + e.what());
            }
        }
        if (target_node.is_object()) {
            return target_node.size();
        } else {
            return std::nullopt;
        }
    } else {
        throwIfNotLegacyWithLua("json_object_length");
        json result = _lua_script_manager->execute_script("json_object_length", {key}, {path});
        if (result.is_null()) {
            return std::nullopt;
        }
        if (result.is_number_integer()) {
            long long count = result.get<long long>();
            if (count < 0) {
                throw RedisCommandException("json_object_length", "Lua script returned negative count for key '" + key + "', path '" + path + "'.");
            }
            return static_cast<size_t>(count);
        }
        throw RedisCommandException("json_object_length", "Unexpected reply format from Lua script for key '" + key + "', path '" + path + "'. Expected integer or null, got: " + result.dump());
    }
}

void RedisJSONClient::patch_json(const std::string& key, const json& patch_operations) {
    json current_doc;
    SetOptions opts;
    try {
        current_doc = get_json(key);
    } catch (const PathNotFoundException& ) {
        current_doc = json(nullptr);
    }
    json patched_doc = current_doc.patch(patch_operations);
    set_json(key, patched_doc, opts);
}

json RedisJSONClient::non_atomic_get_set(const std::string& key, const std::string& path_str,
                                         const json& new_value) {
    if (_is_swss_mode) {
        json doc = _get_document_for_modification(key);
        json old_value_at_path = json(nullptr);
        try {
            old_value_at_path = _json_modifier->get(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            // Path doesn't exist, old_value_at_path remains null.
        }
        SetOptions opts;
        _json_modifier->set(doc, _path_parser->parse(path_str), new_value, true , true );
        _set_document_after_modification(key, doc, opts);
        return old_value_at_path;
    } else {
        throwIfNotLegacyWithLua("json_get_set");
        json result = _lua_script_manager->execute_script("json_get_set", {key}, {path_str, new_value.dump()});
        return result;
    }
}

bool RedisJSONClient::non_atomic_compare_set(const std::string& key, const std::string& path_str,
                                            const json& expected_val, const json& new_val) {
    if (_is_swss_mode) {
        json doc;
        try {
            doc = get_json(key);
        } catch (const PathNotFoundException&) {
            return false;
        }
        json current_val_at_path = json(nullptr);
        bool path_existed = true;
        try {
            current_val_at_path = _json_modifier->get(doc, _path_parser->parse(path_str));
        } catch (const PathNotFoundException&) {
            path_existed = false;
        }
        if ((path_existed && current_val_at_path == expected_val) || (!path_existed && expected_val.is_null())) {
            SetOptions opts;
            _json_modifier->set(doc, _path_parser->parse(path_str), new_val, true, true);
            _set_document_after_modification(key, doc, opts);
            return true;
        }
        return false;
    } else {
        throwIfNotLegacyWithLua("json_compare_set");
        json result_json = _lua_script_manager->execute_script("json_compare_set", {key}, {path_str, expected_val.dump(), new_val.dump()});
        if (result_json.is_number_integer()) return result_json.get<int>() == 1;
        throw LuaScriptException("json_compare_set", "Non-integer result: " + result_json.dump());
    }
}

std::vector<std::string> RedisJSONClient::keys_by_pattern(const std::string& pattern) const {
    if (_is_swss_mode) {
        if (!_db_connector) throw RedisJSONException("DBConnector not initialized for SWSS mode.");
        return _db_connector->keys(pattern);
    } else {
        std::vector<std::string> found_keys;
        std::string cursor = "0";
        RedisConnectionManager::RedisConnectionPtr conn;
        do {
            conn = get_legacy_redis_connection();
            RedisReplyPtr reply(static_cast<redisReply*>(
                conn->command("SCAN %s MATCH %s COUNT 100", cursor.c_str(), pattern.c_str())
            ));
             _connection_manager->return_connection(std::move(conn)); // Return connection after command

            if (!reply) {
                throw RedisCommandException("SCAN", "Pattern: " + pattern + ", No reply");
            }
            if (reply->type == REDIS_REPLY_ERROR) {
                 std::string err = reply->str ? reply->str : "Unknown SCAN error";
                throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: " + err);
            }
            if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
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
        } while (cursor != "0");
        return found_keys;
    }
}

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
        document_to_search = get_json(key);
    } catch (const PathNotFoundException& ) {
        return json::array();
    }
    json::array_t results_array;
    find_values_recursive(document_to_search, search_value, results_array);
    return json(results_array);
}

void find_json_paths_recursive(const json& current_node, const std::string& current_path_str, std::vector<std::string>& paths_list) {
    if (current_node.is_object()) {
        for (auto& el : current_node.items()) {
            std::string child_path = current_path_str + "." + PathParser::escape_key_if_needed(el.key());
            paths_list.push_back(child_path);
            find_json_paths_recursive(el.value(), child_path, paths_list);
        }
    } else if (current_node.is_array()) {
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
        document = get_json(key);
    } catch (const PathNotFoundException& ) {
        return {};
    }
    std::vector<std::string> all_paths;
    if (!document.is_null()) {
        all_paths.push_back("$");
        if (!document.empty() || document.is_object() || document.is_array()) {
             find_json_paths_recursive(document, "$", all_paths);
        }
    }
    return all_paths;
}

json RedisJSONClient::_parse_json_reply(const std::string& reply_str, const std::string& context_msg) const {
    try {
        return json::parse(reply_str);
    } catch (const json::parse_error& e) {
        throw JsonParsingException(context_msg + ": " + e.what() + ". Received: " + reply_str);
    }
}

long long RedisJSONClient::json_clear(const std::string& key, const std::string& path) {
    if (key.empty()) {
        throw ArgumentInvalidException("Key cannot be empty for JSON.CLEAR operation.");
    }
    throwIfNotLegacyWithLua("json_clear");
    try {
        json result_json = _lua_script_manager->execute_script("json_clear", {key}, {path});
        if (result_json.is_number_integer()) {
            return result_json.get<long long>();
        } else if (result_json.is_null()) {
            return 0;
        }
        else if (result_json.is_string()) {
            throw RedisJSONException("JSON.CLEAR script returned an unexpected string: " + result_json.get<std::string>());
        }
        else {
            throw TypeMismatchException("JSON.CLEAR: Unexpected result type from Lua script: " + std::string(result_json.type_name()));
        }
    } catch (const LuaScriptException& e) {
        std::string what_str = e.what();
        if (what_str.find("ERR document not found") != std::string::npos) {
            throw PathNotFoundException(key, path);
        }
        throw;
    }
}

} // namespace redisjson
