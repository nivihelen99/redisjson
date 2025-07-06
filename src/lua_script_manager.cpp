#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h" // For RedisConnection
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr
#include "redisjson++/exceptions.h" // For JsonParsingException, LuaScriptException
#include <vector>
#include <string>
#include <algorithm> // For std::all_of
#include <cstring> // For strncmp

namespace redisjson {

// --- Built-in Lua Scripts Definitions ---

// Helper function to parse a JSON path string (e.g., "obj.arr[0].field")
// Returns a table of path segments. Numeric indices are converted to numbers (1-based for Lua).
const std::string LUA_HELPER_PARSE_PATH_FUNC = R"lua(
local function parse_path(path_str)
    local segments = {}
    if path_str == nil or path_str == '$' or path_str == '' then
        return segments -- Root path
    end
    path_str = path_str:gsub('^%$%.', ''):gsub('^%$%[', '[')

    local current_pos = 1
    while current_pos <= #path_str do
        local key_match = path_str:match('^([^%.%[]+)', current_pos)
        if key_match then
            -- If key_match itself contains escaped dots or brackets, this simple parser won't handle it.
            -- Assuming simple keys for now, or keys are pre-escaped/quoted if needed by a more complex path spec.
            table.insert(segments, key_match)
            current_pos = current_pos + #key_match
        else
            -- This case implies we are at a bracket or end of string after a dot.
            -- If not at a bracket, and not end of string, it might be an error or complex path.
        end

        if current_pos <= #path_str then
            local char = path_str:sub(current_pos, current_pos)
            if char == '.' then
                if current_pos == #path_str then -- Trailing dot
                    --redis.log(redis.LOG_WARNING, "Path ends with a dot: " .. path_str)
                    break -- Path ends with a dot, ignore.
                end
                current_pos = current_pos + 1 -- Skip dot
            elseif char == '[' then
                local end_bracket_pos = path_str:find(']', current_pos)
                if not end_bracket_pos then
                    return redis.error_reply("ERR_PATH Malformed path: Unmatched '[' in path: " .. path_str)
                end
                local index_str = path_str:sub(current_pos + 1, end_bracket_pos - 1)
                local index_num = tonumber(index_str)
                if index_num == nil then
                     return redis.error_reply("ERR_PATH Malformed path: Non-numeric index '" .. index_str .. "' in path: " .. path_str)
                end
                table.insert(segments, index_num + 1) -- Lua arrays are 1-indexed
                current_pos = end_bracket_pos + 1
            else
                -- Unexpected character in path
                return redis.error_reply("ERR_PATH Malformed path: Unexpected character '" .. char .. "' at pos " .. current_pos .. " in path: " .. path_str)
            end
        end
    end
    return segments
end
)lua";

const std::string LUA_HELPER_GET_VALUE_AT_PATH_FUNC = R"lua(
local function get_value_at_path(doc, path_segments)
    local current = doc
    for i, segment in ipairs(path_segments) do
        if type(current) ~= 'table' then
            return nil -- Path leads into a non-table value
        end
        current = current[segment]
        if current == nil then
            return nil -- Path segment not found
        end
    end
    return current
end
)lua";

const std::string LUA_HELPER_SET_VALUE_AT_PATH_FUNC = R"lua(
local function set_value_at_path(doc, path_segments, value_to_set, create_path_flag)
    local current = doc
    for i = 1, #path_segments - 1 do
        local segment = path_segments[i]
        if type(current) ~= 'table' then
            return false, 'Path segment ' .. tostring(segment) .. ' is not a table/array'
        end
        if current[segment] == nil or type(current[segment]) ~= 'table' then
            if create_path_flag then
                local next_segment = path_segments[i+1]
                if type(next_segment) == 'number' then
                    current[segment] = {} 
                else
                    current[segment] = {} 
                end
            else
                return false, 'Path segment ' .. tostring(segment) .. ' not found and create_path is false'
            end
        end
        current = current[segment]
    end

    local final_segment = path_segments[#path_segments]
    if type(current) ~= 'table' then
         return false, 'Final path leads to a non-table parent for segment ' .. tostring(final_segment)
    end
    current[final_segment] = value_to_set
    return true, 'OK'
end
)lua";

const std::string LUA_HELPER_DEL_VALUE_AT_PATH_FUNC = R"lua(
local function del_value_at_path(doc, path_segments)
    local current = doc
    if #path_segments == 0 then 
        return false, 'Cannot delete root object/document using path DEL; use DEL key command'
    end

    for i = 1, #path_segments - 1 do
        local segment = path_segments[i]
        if type(current) ~= 'table' then
            return false, 'Path segment ' .. tostring(segment) .. ' is not a table/array'
        end
        current = current[segment]
        if current == nil then
            return true, 'Intermediate path segment ' .. tostring(segment) .. ' not found, nothing to delete' 
        end
    end

    local final_segment = path_segments[#path_segments]
    if type(current) == 'table' then
        if current[final_segment] ~= nil then
            current[final_segment] = nil
            return true, 'OK'
        else
            return true, 'Final path segment ' .. tostring(final_segment) .. ' not found, nothing to delete'
        end
    else
        return false, 'Final path leads to a non-table parent for segment ' .. tostring(final_segment)
    end
end
)lua";

const std::string LUA_COMMON_HELPERS = LUA_HELPER_PARSE_PATH_FUNC + LUA_HELPER_GET_VALUE_AT_PATH_FUNC + LUA_HELPER_SET_VALUE_AT_PATH_FUNC + LUA_HELPER_DEL_VALUE_AT_PATH_FUNC;

const std::string LuaScriptManager::JSON_PATH_GET_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return nil end
    local current_doc, err = cjson.decode(current_json_str)
    if not current_doc then return redis.error_reply('ERR_DECODE Key ' .. key .. ': ' .. (err or 'unknown error')) end
    if path_str == '$' or path_str == '' then return cjson.encode(current_doc) end
    local path_segments = parse_path(path_str)
    if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
    local value_at_path = get_value_at_path(current_doc, path_segments)
    return cjson.encode(value_at_path) -- cjson.encode(nil) is "null"
)lua";

const std::string LuaScriptManager::JSON_PATH_SET_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local new_value_json_str = ARGV[2]
    local condition = ARGV[3]
    local ttl_str = ARGV[4]
    local create_path_str = ARGV[5]
    local create_path_flag = (create_path_str == "true")

    local current_json_str = redis.call('GET', key)
    local current_doc
    local path_exists = false

    if current_json_str then
        local err
        current_doc, err = cjson.decode(current_json_str)
        if not current_doc then return redis.error_reply('ERR_DECODE Existing JSON: ' .. (err or 'unknown error')) end
        if path_str == '$' or path_str == '' then path_exists = true else
            local temp_path_segments = parse_path(path_str)
            if temp_path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string for check: ' .. path_str) end
            if get_value_at_path(current_doc, temp_path_segments) ~= nil then path_exists = true end
        end
    else
        if condition == 'XX' then return false end
        current_doc = {}
        if path_str == '$' or path_str == '' then path_exists = true end 
    end

    if condition == 'NX' and path_exists then return false end
    if condition == 'XX' and not path_exists then return false end

    local new_value, err_val = cjson.decode(new_value_json_str)
    if not new_value and new_value_json_str ~= 'null' then return redis.error_reply('ERR_DECODE_ARG New value: '.. (err_val or 'unknown error')) end

    if path_str == '$' or path_str == '' then 
        if type(new_value) ~= 'table' and new_value_json_str ~= 'null' then return redis.error_reply('ERR_ROOT_TYPE Root must be object/array/null') end
        current_doc = new_value 
    else
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string for set: ' .. path_str) end
        local success, err_set = set_value_at_path(current_doc, path_segments, new_value, create_path_flag)
        if not success then return redis.error_reply('ERR_SET_PATH ' .. err_set) end
    end

    local new_doc_json_str, err_enc = cjson.encode(current_doc)
    if not new_doc_json_str then return redis.error_reply('ERR_ENCODE Document: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, new_doc_json_str)

    local ttl = tonumber(ttl_str)
    if ttl and ttl > 0 then redis.call('EXPIRE', key, ttl) end
    return true
)lua";

const std::string LuaScriptManager::JSON_PATH_DEL_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return 0 end
    local current_doc, err = cjson.decode(current_json_str)
    if not current_doc then return redis.error_reply('ERR_DECODE JSON: ' .. (err or 'unknown error')) end

    if path_str == '$' or path_str == '' then return redis.call('DEL', key) end

    local path_segments = parse_path(path_str)
    if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
    local success, msg = del_value_at_path(current_doc, path_segments)
    if not success then return redis.error_reply('ERR_DEL_PATH ' .. msg) end
    if msg ~= 'OK' then return 0 end

    local new_doc_json_str, err_enc = cjson.encode(current_doc)
    if not new_doc_json_str then return redis.error_reply('ERR_ENCODE Deleted doc: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, new_doc_json_str)
    return 1
)lua";

const std::string LuaScriptManager::JSON_PATH_TYPE_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return nil end
    local current_doc, err = cjson.decode(current_json_str)
    if not current_doc then return redis.error_reply('ERR_DECODE JSON: ' .. (err or 'unknown error')) end
    
    local value_at_path
    if path_str == '$' or path_str == '' then value_at_path = current_doc else
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
        value_at_path = get_value_at_path(current_doc, path_segments)
    end

    if value_at_path == nil then return nil end -- Path not found or explicit JSON null handled by cjson.encode(nil)
    if value_at_path == cjson.null then return "null" end

    local lua_type = type(value_at_path)
    if lua_type == 'table' then
        local is_array = true; local n = 0
        for k,v in pairs(value_at_path) do n = n + 1; if type(k) ~= 'number' or k < 1 or k > n then is_array = false; break; end end
        if n == 0 and next(value_at_path) == nil then return "array" end -- Empty table is empty array by convention here
        if is_array and #value_at_path == n then return "array" else return "object" end
    elseif lua_type == 'string' then return "string"
    elseif lua_type == 'number' then if math.floor(value_at_path) == value_at_path then return "integer" else return "number" end
    elseif lua_type == 'boolean' then return "boolean" end
    return nil 
)lua";

const std::string LuaScriptManager::JSON_ARRAY_APPEND_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local value_json_str = ARGV[2]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return redis.error_reply('ERR_NOKEY Key not found') end
    local doc, err = cjson.decode(current_json_str)
    if not doc then return redis.error_reply('ERR_DECODE Invalid JSON: ' .. (err or 'unknown')) end
    local value_to_append, err_val = cjson.decode(value_json_str)
    if not value_to_append and value_json_str ~= 'null' then return redis.error_reply('ERR_DECODE_ARG Value: ' .. (err_val or 'unknown')) end

    local target_array_ref = doc 
    if path_str ~= '$' and path_str ~= '' then
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
        target_array_ref = get_value_at_path(doc, path_segments)
    end

    if target_array_ref == nil then return redis.error_reply('ERR_NOPATH Path not found') end
    if type(target_array_ref) ~= 'table' then return "ERR_NOT_ARRAY" end 
    table.insert(target_array_ref, value_to_append)
        
    local new_doc_json_str, err_enc = cjson.encode(doc)
    if not new_doc_json_str then return redis.error_reply('ERR_ENCODE Document: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, new_doc_json_str)
    return #target_array_ref
)lua";

const std::string LuaScriptManager::JSON_ARRAY_PREPEND_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local value_json_str = ARGV[2]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return redis.error_reply('ERR_NOKEY Key not found') end
    local doc, err = cjson.decode(current_json_str)
    if not doc then return redis.error_reply('ERR_DECODE Invalid JSON: ' .. (err or 'unknown')) end
    local value_to_prepend, err_val = cjson.decode(value_json_str)
    if not value_to_prepend and value_json_str ~= 'null' then return redis.error_reply('ERR_DECODE_ARG Value: ' .. (err_val or 'unknown')) end
    
    local target_array_ref = doc
    if path_str ~= '$' and path_str ~= '' then
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
        target_array_ref = get_value_at_path(doc, path_segments)
    end

    if target_array_ref == nil then return redis.error_reply('ERR_NOPATH Path not found') end
    if type(target_array_ref) ~= 'table' then return "ERR_NOT_ARRAY" end
    table.insert(target_array_ref, 1, value_to_prepend)
    
    local new_doc_json_str, err_enc = cjson.encode(doc)
    if not new_doc_json_str then return redis.error_reply('ERR_ENCODE Document: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, new_doc_json_str)
    return #target_array_ref
)lua";

const std::string LuaScriptManager::JSON_ARRAY_POP_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local index_str = ARGV[2] 
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return nil end 
    local doc, err = cjson.decode(current_json_str)
    if not doc then return redis.error_reply('ERR_DECODE Invalid JSON: ' .. (err or 'unknown')) end

    local target_array_ref = doc
    if path_str ~= '$' and path_str ~= '' then
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
        target_array_ref = get_value_at_path(doc, path_segments)
    end

    if target_array_ref == nil or type(target_array_ref) ~= 'table' then return nil end 
    local index = tonumber(index_str)
    if index == nil then return redis.error_reply('ERR_INDEX Invalid index: not a number') end

    -- Adjust C++ index (0-based, -1 for last) to Lua 1-based index
    local len = #target_array_ref
    if index == -1 then index = len 
    elseif index >= 0 and index < len then index = index + 1
    else return nil -- Index out of bounds based on C++ 0-based convention
    end

    if index < 1 or index > len or len == 0 then return nil end

    local popped_value = table.remove(target_array_ref, index)
    local new_doc_json_str, err_enc = cjson.encode(doc)
    if not new_doc_json_str then return redis.error_reply('ERR_ENCODE Document: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, new_doc_json_str)
    return cjson.encode(popped_value) 
)lua";

const std::string LuaScriptManager::JSON_ARRAY_LENGTH_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local current_json_str = redis.call('GET', key)
    if not current_json_str then return nil end 
    local doc, err = cjson.decode(current_json_str)
    if not doc then return redis.error_reply('ERR_DECODE Invalid JSON: ' .. (err or 'unknown')) end

    local target_array_ref = doc
    if path_str ~= '$' and path_str ~= '' then
        local path_segments = parse_path(path_str)
        if path_segments == nil then return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str) end
        target_array_ref = get_value_at_path(doc, path_segments)
    end
    if target_array_ref == nil or type(target_array_ref) ~= 'table' then return nil end 
    return #target_array_ref
)lua";

const std::string LuaScriptManager::ATOMIC_JSON_GET_SET_PATH_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local new_value_json_str = ARGV[2]
    local current_json_str = redis.call('GET', key)
    local current_doc
    local old_value_encoded = cjson.encode(nil) 

    if not current_json_str then current_doc = {} else
        local err_dec_curr
        current_doc, err_dec_curr = cjson.decode(current_json_str)
        if not current_doc then return redis.error_reply('ERR_DECODE Existing JSON: ' .. (err_dec_curr or 'unknown')) end
        if path_str == '$' or path_str == '' then old_value_encoded = cjson.encode(current_doc) else
            local path_segments_old = parse_path(path_str)
            if path_segments_old == nil then return redis.error_reply('ERR_PATH Invalid path for old value: ' .. path_str) end
            local old_value = get_value_at_path(current_doc, path_segments_old)
            old_value_encoded = cjson.encode(old_value)
        end
    end

    local new_value, err_val = cjson.decode(new_value_json_str)
    if not new_value and new_value_json_str ~= 'null' then return redis.error_reply('ERR_DECODE_ARG New value: ' .. (err_val or 'unknown')) end
    
    if path_str == '$' or path_str == '' then 
         if type(new_value) ~= 'table' and new_value_json_str ~= 'null' then return redis.error_reply('ERR_ROOT_TYPE Root must be object/array/null') end
        current_doc = new_value
    else
        local path_segments_set = parse_path(path_str)
        if path_segments_set == nil then return redis.error_reply('ERR_PATH Invalid path for set: ' .. path_str) end
        local success, err_set = set_value_at_path(current_doc, path_segments_set, new_value, true) 
        if not success then return redis.error_reply('ERR_SET_PATH ' .. err_set) end
    end
    
    local final_doc_str, err_enc = cjson.encode(current_doc)
    if not final_doc_str then return redis.error_reply('ERR_ENCODE Final doc: ' .. (err_enc or 'unknown')) end
    redis.call('SET', key, final_doc_str)
    return old_value_encoded
)lua";

const std::string LuaScriptManager::ATOMIC_JSON_COMPARE_SET_PATH_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local expected_value_json_str = ARGV[2]
    local new_value_json_str = ARGV[3]
    local current_json_str = redis.call('GET', key)
    local current_doc
    local actual_value_at_path

    if not current_json_str then
        if expected_value_json_str == cjson.encode(nil) then actual_value_at_path = nil else return 0 end
        current_doc = {} 
    else
        local err_dec_curr
        current_doc, err_dec_curr = cjson.decode(current_json_str)
        if not current_doc then return redis.error_reply('ERR_DECODE Existing JSON: ' .. (err_dec_curr or 'unknown')) end
        if path_str == '$' or path_str == '' then actual_value_at_path = current_doc else
            local path_segments_get = parse_path(path_str)
            if path_segments_get == nil then return redis.error_reply('ERR_PATH Invalid path for get: ' .. path_str) end
            actual_value_at_path = get_value_at_path(current_doc, path_segments_get)
        end
    end

    local actual_value_encoded = cjson.encode(actual_value_at_path)

    if actual_value_encoded == expected_value_json_str then
        local new_value, err_val = cjson.decode(new_value_json_str)
        if not new_value and new_value_json_str ~= 'null' then return redis.error_reply('ERR_DECODE_ARG New value CAS: ' .. (err_val or 'unknown')) end
        
        if path_str == '$' or path_str == '' then 
            if type(new_value) ~= 'table' and new_value_json_str ~= 'null' then return redis.error_reply('ERR_ROOT_TYPE Root CAS: object/array/null') end
            current_doc = new_value
        else
            local path_segments_set = parse_path(path_str)
            if path_segments_set == nil then return redis.error_reply('ERR_PATH Invalid path for set CAS: ' .. path_str) end
            local success, err_set = set_value_at_path(current_doc, path_segments_set, new_value, true)
            if not success then return redis.error_reply('ERR_SET_PATH CAS: ' .. err_set) end
        end
        local final_doc_str, err_enc = cjson.encode(current_doc)
        if not final_doc_str then return redis.error_reply('ERR_ENCODE Final doc CAS: ' .. (err_enc or 'unknown')) end
        redis.call('SET', key, final_doc_str)
        return 1
    else
        return 0
    end
)lua";

LuaScriptManager::LuaScriptManager(RedisConnectionManager* conn_manager)
    : connection_manager_(conn_manager) {
    if (!conn_manager) {
        throw std::invalid_argument("RedisConnectionManager cannot be null for LuaScriptManager.");
    }
}

LuaScriptManager::~LuaScriptManager() {
    // Destructor remains the same
}

void LuaScriptManager::load_script(const std::string& name, const std::string& script_body) {
    std::cout << "LOG: LuaScriptManager::load_script() - Entry for script name: '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    if (name.empty() || script_body.empty()) {
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - Script name or body is empty. Name: '" << name << "'. Throwing." << std::endl;
        throw std::invalid_argument("Script name and body cannot be empty.");
    }

    std::cout << "LOG: LuaScriptManager::load_script() - Getting connection for SCRIPT LOAD '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    RedisConnectionManager::RedisConnectionPtr conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();

    if (!conn || !conn->is_connected()) {
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - Failed to get valid Redis connection for SCRIPT LOAD '" << name << "'. Conn is " << (conn ? "not null but not connected" : "null") << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection if it's not null
        throw ConnectionException("Failed to get valid Redis connection for SCRIPT LOAD.");
    }
    std::cout << "LOG: LuaScriptManager::load_script() - Got connection for SCRIPT LOAD '" << name << "'. Host: " << conn->get_host() << ":" << conn->get_port() << ". Thread ID: " << std::this_thread::get_id() << std::endl;

    std::cout << "LOG: LuaScriptManager::load_script() - Calling conn->command('SCRIPT LOAD ...') for script '" << name << "'. First 30 chars of body: " << script_body.substr(0, 30) << (script_body.length() > 30 ? "..." : "") << ". Thread ID: " << std::this_thread::get_id() << std::endl;
    RedisReplyPtr reply(static_cast<redisReply*>(conn->command("SCRIPT LOAD %s", script_body.c_str())));

    std::string error_context_for_throw;
    if (conn && conn->get_context()) { // Check if conn and its context are valid
        error_context_for_throw = " (hiredis context error: " + std::string(conn->get_context()->errstr ? conn->get_context()->errstr : "unknown") +
                                  ", code: " + std::to_string(conn->get_context()->err) + ")";
    } else if (conn) {
        error_context_for_throw = " (hiredis context is null, connection might be bad)";
    } else {
        error_context_for_throw = " (RedisConnection object is null)";
    }


    if (!reply) {
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - conn->command('SCRIPT LOAD') for '" << name << "' returned null reply. Error context:" << error_context_for_throw << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "No reply from Redis for script '" + name + "'" + error_context_for_throw);
    }
    std::cout << "LOG: LuaScriptManager::load_script() - conn->command('SCRIPT LOAD') for '" << name << "' returned. Reply type: " << reply->type << ". Thread ID: " << std::this_thread::get_id() << std::endl;

    std::string sha1_hash;
    if (reply->type == REDIS_REPLY_STRING) {
        sha1_hash = std::string(reply->str, reply->len);
        std::cout << "LOG: LuaScriptManager::load_script() - Script '" << name << "' loaded successfully. SHA1: " << sha1_hash << ". Thread ID: " << std::this_thread::get_id() << std::endl;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = (reply->str ? std::string(reply->str, reply->len) : "Unknown Redis error");
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' failed with REDIS_REPLY_ERROR: " << err_msg << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "Error for script '" + name + "': " + err_msg);
    } else {
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' returned unexpected reply type: " << reply->type << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "Unexpected reply type for script '" + name + "': " + std::to_string(reply->type));
    }

    if (sha1_hash.empty()) {
        std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' resulted in empty SHA1 hash. Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
         // conn_guard will release connection
         throw RedisCommandException("SCRIPT LOAD", "Failed to load script '" + name + "', SHA1 hash is empty.");
    }

    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        script_shas_[name] = sha1_hash;
        std::cout << "LOG: LuaScriptManager::load_script() - Cached SHA1 for script '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    }

    // Connection is returned by RedisConnectionPtr conn_guard going out of scope.
    // No explicit call to return_connection needed here for logging its return.
    std::cout << "LOG: LuaScriptManager::load_script() - Exiting for script name: '" << name << "'. Connection will be returned by RAII. Thread ID: " << std::this_thread::get_id() << std::endl;
}

// Initialize the static map of script definitions
const std::map<std::string, const std::string*> LuaScriptManager::SCRIPT_DEFINITIONS = {
    {"json_path_get", &LuaScriptManager::JSON_PATH_GET_LUA},
    {"json_path_set", &LuaScriptManager::JSON_PATH_SET_LUA},
    {"json_path_del", &LuaScriptManager::JSON_PATH_DEL_LUA},
    {"json_path_type", &LuaScriptManager::JSON_PATH_TYPE_LUA},
    {"json_array_append", &LuaScriptManager::JSON_ARRAY_APPEND_LUA},
    {"json_array_prepend", &LuaScriptManager::JSON_ARRAY_PREPEND_LUA},
    {"json_array_pop", &LuaScriptManager::JSON_ARRAY_POP_LUA},
    {"json_array_length", &LuaScriptManager::JSON_ARRAY_LENGTH_LUA},
    {"atomic_json_get_set_path", &LuaScriptManager::ATOMIC_JSON_GET_SET_PATH_LUA},
    {"atomic_json_compare_set_path", &LuaScriptManager::ATOMIC_JSON_COMPARE_SET_PATH_LUA}
    // Add any other scripts here if they are defined as static const std::string members
};

const std::string* LuaScriptManager::get_script_body_by_name(const std::string& name) const {
    auto it = SCRIPT_DEFINITIONS.find(name);
    if (it != SCRIPT_DEFINITIONS.end()) {
        return it->second;
    }
    return nullptr;
}

json LuaScriptManager::redis_reply_to_json(redisReply* reply) const {
    if (!reply) return json(nullptr);

    switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS: {
            std::string reply_str(reply->str, reply->len);
            // Heuristic to decide if it's a JSON string or a simple status/error string from Lua
            if ((!reply_str.empty() && (reply_str[0] == '{' || reply_str[0] == '[' || reply_str[0] == '"')) ||
                reply_str == "null" || reply_str == "true" || reply_str == "false") {
                try {
                    return json::parse(reply_str);
                } catch (const json::parse_error& e) {
                    // It looked like JSON but failed to parse, this is an issue.
                    throw JsonParsingException("Failed to parse script string output as JSON: " + std::string(e.what()) + ", content: " + reply_str);
                }
            }
            // Check if it's a number string
            bool is_numeric = true;
            if (reply_str.empty()) is_numeric = false;
            else {
                size_t i = 0;
                if (reply_str[0] == '-') i = 1;
                for (; i < reply_str.length(); ++i) {
                    if (!std::isdigit(reply_str[i]) && reply_str[i] != '.') {
                        is_numeric = false;
                        break;
                    }
                }
            }
            if (is_numeric) {
                 try {
                    // Try to parse as number if it looks like one (e.g. "123", "0.5")
                    // This might still fail for things like "1.2.3"
                    // A stricter check might be needed or rely on Lua to return proper types / JSON strings.
                    size_t processed_chars = 0;
                    double num_val = std::stod(reply_str, &processed_chars);
                    if (processed_chars == reply_str.length()){ // ensure full string was consumed
                        if (num_val == static_cast<long long>(num_val)) return json(static_cast<long long>(num_val));
                        return json(num_val);
                    }
                 } catch (const std::exception&) { /* Not a valid number, treat as string below */ }
            }
            // Otherwise, treat as a simple string value (e.g. "OK", "ERR_NOT_ARRAY")
            return json(reply_str);
        }
        case REDIS_REPLY_INTEGER:
            return json(reply->integer);
        case REDIS_REPLY_NIL:
            return json(nullptr);
        case REDIS_REPLY_ARRAY: {
            json::array_t arr;
            for (size_t i = 0; i < reply->elements; ++i) {
                arr.push_back(redis_reply_to_json(reply->element[i]));
            }
            return arr;
        }
        case REDIS_REPLY_ERROR:
            throw LuaScriptException("<unknown script, error from reply>", std::string(reply->str, reply->len));
        default:
            throw RedisCommandException("EVALSHA/SCRIPT", "Unexpected Redis reply type: " + std::to_string(reply->type));
    }
}

json LuaScriptManager::execute_script(const std::string& name,
                                    const std::vector<std::string>& keys,
                                    const std::vector<std::string>& args) {
    std::string sha1_hash;
    bool attempted_on_demand_load = false;

    // Loop to handle on-demand loading. In practice, it runs once or twice.
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::unique_lock<std::mutex> lock(cache_mutex_); // Use unique_lock for manual unlock/relock
        auto it = script_shas_.find(name);
        if (it != script_shas_.end()) {
            sha1_hash = it->second;
            break; // Found SHA, exit loop
        }

        // SHA not found. If already tried on-demand load (i.e. attempt > 0), something is wrong.
        if (attempt > 0) {
            throw LuaScriptException(name, "Script SHA not found in cache even after on-demand load attempt for: " + name);
        }

        // First attempt (attempt == 0) and SHA not found: try on-demand load
        lock.unlock(); // Unlock before calling load_script

        std::cerr << "INFO: Lua script '" << name << "' not found in cache. Attempting on-demand load." << std::endl;
        attempted_on_demand_load = true; // Mark that we are trying/tried
        const std::string* script_body_ptr = get_script_body_by_name(name);

        if (!script_body_ptr) {
            throw LuaScriptException(name, "Script body not found for on-demand loading of script: " + name);
        }

        try {
            load_script(name, *script_body_ptr); // This will populate script_shas_ if successful
            std::cerr << "INFO: Successfully loaded Lua script '" << name << "' on demand." << std::endl;
            // Loop will continue to re-acquire lock and find SHA.
        } catch (const RedisJSONException& e) {
            std::cerr << "ERROR: Failed to load Lua script '" << name << "' on demand: " << e.what() << std::endl;
            throw LuaScriptException(name, "Failed to load script '" + name + "' on demand: " + std::string(e.what()));
        }
        // After load_script, loop iterates, re-locks and re-checks script_shas_.
    }

    if (sha1_hash.empty()) { // Should be caught by the check inside the loop if load failed
        throw LuaScriptException(name, "Failed to obtain SHA1 for script: " + name + " after load attempts.");
    }

    // Use RedisConnectionPtr which includes the custom deleter
    RedisConnectionManager::RedisConnectionPtr conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();
     if (!conn || !conn->is_connected()) {
        throw ConnectionException("Failed to get valid Redis connection for EVALSHA.");
    }

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

    RedisReplyPtr reply(static_cast<redisReply*>(conn->command_argv(argv_c.size(), argv_c.data(), argv_len.data())));

    if (!reply) {
         throw RedisCommandException("EVALSHA", "No reply from Redis (connection error: " + (conn->get_context() ? std::string(conn->get_context()->errstr) : "unknown") + ") for script " + name);
    }

    if (reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, "NOSCRIPT", 8) == 0) {
        std::string noscript_error_msg = std::string(reply->str, reply->len);
        // In a more robust system, one might attempt to reload the specific script here.
        // For now, just propagate the error clearly.
        throw LuaScriptException(name, "Script not found on server (NOSCRIPT): " + noscript_error_msg + ". Consider reloading scripts if SCRIPT FLUSH occurred.");
    }
    // If the reply is an error from the script itself (not NOSCRIPT), redis_reply_to_json will throw LuaScriptException.
    return redis_reply_to_json(reply.get());
}

void LuaScriptManager::preload_builtin_scripts() {
    // Iterate over the SCRIPT_DEFINITIONS map to preload all defined scripts.
    std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Entry. Thread ID: " << std::this_thread::get_id() << std::endl;
    int success_count = 0;
    int fail_count = 0;
    int script_idx = 0;

    for (const auto& pair : SCRIPT_DEFINITIONS) {
        script_idx++;
        const std::string& script_name = pair.first;
        const std::string* script_body = pair.second;
        std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Attempting to load script " << script_idx << "/" << SCRIPT_DEFINITIONS.size() << ": '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
        try {
            load_script(script_name, *script_body);
            std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Successfully preloaded script: '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
            success_count++;
        } catch (const RedisJSONException& e) {
            fail_count++;
            std::cout << "ERROR_LOG: LuaScriptManager::preload_builtin_scripts() - Failed to preload Lua script '" << script_name << "' (RedisJSONException): " << e.what() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
        } catch (const std::exception& e) {
            fail_count++;
            std::cout << "ERROR_LOG: LuaScriptManager::preload_builtin_scripts() - Unexpected std::exception while preloading Lua script '" << script_name << "': " << e.what() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
        }
        #ifndef NDEBUG
        catch (...) {
            fail_count++;
            std::cout << "ERROR_LOG: LuaScriptManager::preload_builtin_scripts() - Unknown error while preloading Lua script '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
        }
        #endif
        std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Finished attempt for script '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    }

    if (fail_count > 0) {
        std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Preloading completed. Success: " << success_count
                  << ", Failed: " << fail_count << " (out of " << SCRIPT_DEFINITIONS.size() << " total). Thread ID: " << std::this_thread::get_id() << std::endl;
    } else {
        std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Successfully preloaded all " << success_count << " built-in Lua scripts. Thread ID: " << std::this_thread::get_id() << std::endl;
    }
    std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Exit. Thread ID: " << std::this_thread::get_id() << std::endl;
}

bool LuaScriptManager::is_script_loaded(const std::string& name) const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return script_shas_.count(name) > 0;
}

void LuaScriptManager::clear_all_scripts_cache() {
    // Use RedisConnectionPtr which includes the custom deleter
    RedisConnectionManager::RedisConnectionPtr conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();
    if (!conn || !conn->is_connected()) {
        throw ConnectionException("Failed to get valid Redis connection for SCRIPT FLUSH.");
    }
    RedisReplyPtr reply(static_cast<redisReply*>(conn->command("SCRIPT FLUSH")));
    if (!reply) {
         throw RedisCommandException("SCRIPT FLUSH", "No reply from Redis (connection error: " + (conn->get_context() ? std::string(conn->get_context()->errstr) : "unknown") + ")");
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = std::string(reply->str, reply->len);
        throw RedisCommandException("SCRIPT FLUSH", err_msg);
    }
    clear_local_script_cache();
}

void LuaScriptManager::clear_local_script_cache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    script_shas_.clear();
}

} // namespace redisjson
