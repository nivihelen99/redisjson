#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h" // For RedisConnection
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr
#include "redisjson++/exceptions.h" // For JsonParsingException, LuaScriptException
#include <vector>
#include <string>
#include <algorithm> // For std::all_of
#include <cstring>   // For strncmp
#include <iostream>  // For std::cout, std::cerr (logging)
#include <thread>    // For std::this_thread::get_id (logging)

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
    path_str = path_str:gsub('^%$%.', ''):gsub('^%$%[', '[') -- Remove $., $[ at start

    local current_pos = 1
    local path_len = #path_str

    -- Enhanced checks for leading/trailing dots and empty initial/final segments
    if path_len > 0 then
        if path_str:sub(1,1) == '.' then
            return redis.error_reply("ERR_PATH Malformed path: Leading dot in path: " .. path_str)
        end
        -- Note: Trailing dot is implicitly handled by loop logic; if a segment ends and then a dot is encountered
        -- with nothing after, it might need specific check or rely on segment_str being empty if current_pos advances past len.
        -- The original code had a 'break' for trailing dot, which we might want to make an error.
    end

    while current_pos <= path_len do
        local next_dot_pos = path_str:find('%.', current_pos)
        local next_bracket_pos = path_str:find('%[', current_pos)
        local segment_end_pos

        -- Determine end of current path segment (before a dot or bracket, or end of string)
        if next_dot_pos and (not next_bracket_pos or next_dot_pos < next_bracket_pos) then
            segment_end_pos = next_dot_pos - 1
        elseif next_bracket_pos and (not next_dot_pos or next_bracket_pos < next_dot_pos) then
            segment_end_pos = next_bracket_pos - 1
        else
            segment_end_pos = path_len -- End of the string
        end

        local segment_str = path_str:sub(current_pos, segment_end_pos)

        if segment_str == '' and current_pos <= path_len and path_str:sub(current_pos, current_pos) ~= '[' then
            -- An empty segment means '..' or an initial '.' not caught, or some other malformation.
            -- Allow empty segment if it's immediately followed by '[' (e.g. path like '[0]' for root array access)
            -- This specific case (root array access like "[0]") is typically handled by path_str:gsub at start or by specific logic.
            -- For now, let's assume path like "[0]" is not passed to this generic segment parser part or is pre-processed.
            -- If `segment_str` is empty here, it implies an issue like `a..b` or `a.[0]`.
            return redis.error_reply("ERR_PATH Malformed path: Empty segment found in path: '" .. path_str .. "' near position " .. current_pos)
        end

        if segment_str ~= '' then -- Only add non-empty segments if we are not parsing a root array index
             table.insert(segments, segment_str)
        end
        current_pos = segment_end_pos + 1

        if current_pos <= path_len then
            local char_at_current_pos = path_str:sub(current_pos, current_pos)
            if char_at_current_pos == '.' then
                current_pos = current_pos + 1 -- Consume the dot
                if current_pos > path_len or path_str:sub(current_pos, current_pos) == '.' or path_str:sub(current_pos, current_pos) == '[' then
                    -- Path ends with a dot, or has '..', or '.['
                    return redis.error_reply("ERR_PATH Malformed path: Invalid sequence after dot in path: '" .. path_str .. "' near position " .. current_pos)
                end
            elseif char_at_current_pos == '[' then
                -- If segment_str was empty, it means path started with '[' or was like '.['
                -- This part handles the bracket and its content.
                local end_bracket_pos = path_str:find(']', current_pos)
                if not end_bracket_pos then
                    return redis.error_reply("ERR_PATH Malformed path: Unmatched '[' in path: " .. path_str)
                end
                local index_str = path_str:sub(current_pos + 1, end_bracket_pos - 1)
                if index_str == '' then
                    return redis.error_reply("ERR_PATH Malformed path: Empty index '[]' in path: " .. path_str)
                end
                local index_num = tonumber(index_str)
                if index_num == nil then
                     return redis.error_reply("ERR_PATH Malformed path: Non-numeric index '" .. index_str .. "' in path: " .. path_str)
                end
                -- If the previous segment was a string key, it's already added.
                -- If path was just "[0]", segment_str would be empty. Add index to segments.
                -- If segments is empty and we are parsing an index, it means path like "[0].field"
                if segment_str == '' and #segments == 0 then
                    -- This handles paths that *start* with an index, e.g. "[0].name" after initial $ stripping
                    -- The previous logic would have added an empty string for segment_str if path was like "[0]"
                    -- We need to ensure that if segment_str is empty, we don't just skip.
                    -- This should be fine as table.insert(segments, index_num + 1) happens below.
                elseif segment_str == '' and #segments > 0 then
                     -- This means something like "key.[]" which is invalid. The previous segment was "key".
                     -- An empty string segment followed by '[' should be an error.
                     return redis.error_reply("ERR_PATH Malformed path: Invalid '[]' after non-empty segment in path: " .. path_str)
                end

                table.insert(segments, index_num + 1) -- Lua arrays are 1-indexed
                current_pos = end_bracket_pos + 1
            else
                -- This case should ideally not be reached if segment parsing is correct.
                -- It implies a character that is not a dot or bracket, but segment_end_pos was calculated before it.
                return redis.error_reply("ERR_PATH Malformed path: Unexpected character '" .. char_at_current_pos .. "' at pos " .. current_pos .. " in path: " .. path_str)
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
    if value_at_path == nil then
        return "[]" -- Return empty array string literal for path not found
    else
        return cjson.encode({value_at_path}) -- Wrap found value in an array
    end
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
    if type(target_array_ref) ~= 'table' then return redis.error_reply('ERR_NOT_ARRAY Path points to a non-array type') end 
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
    if type(target_array_ref) ~= 'table' then return redis.error_reply('ERR_NOT_ARRAY Path points to a non-array type') end
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

const std::string LuaScriptManager::JSON_SPARSE_MERGE_LUA = R"lua(
    local key = KEYS[1]
    local changes_json_str = ARGV[1]

    local changes_doc, err_changes = cjson.decode(changes_json_str)
    if not changes_doc then
        return redis.error_reply('ERR_DECODE_ARG Invalid JSON in changes argument: ' .. (err_changes or 'unknown error'))
    end
    if type(changes_doc) ~= 'table' then
        return redis.error_reply('ERR_ARG_TYPE Changes argument must be a JSON object')
    end
    -- Ensure it's not an array-like table if cjson.decode might produce one for top-level array string
    local changes_is_object = false
    if next(changes_doc) == nil then -- Empty table could be object {}
        changes_is_object = true
    else
        for k, _ in pairs(changes_doc) do
            if type(k) ~= 'number' then
                changes_is_object = true
                break
            end
        end
        -- If all keys are numbers, it might still be an object like {1: "val"}
        -- A simple heuristic: if it has non-numeric keys, it's an object.
        -- If all keys are numeric and sequential from 1, it's likely an array.
        -- For this merge, we strictly expect an object. If cjson decodes `[]` to a table with numeric keys 1..N,
        -- we should reject it.
        if not changes_is_object then -- Still could be an array
            local max_idx = 0
            local count = 0
            for k, _ in pairs(changes_doc) do
                if type(k) == 'number' and k >= 1 and math.floor(k) == k then
                    if k > max_idx then max_idx = k end
                    count = count + 1
                else
                    changes_is_object = true -- Found a non-numeric key
                    break
                end
            end
            if not changes_is_object and count > 0 and max_idx == count then -- It's a dense array
                 return redis.error_reply('ERR_ARG_TYPE Changes argument must be a JSON object, not an array')
            elseif not changes_is_object and count == 0 and max_idx == 0 then -- Empty table is fine as an object {}
                 changes_is_object = true
            elseif not changes_is_object and count > 0 and max_idx ~= count then -- Sparse array like table, treat as object
                 changes_is_object = true
            end
        end
    end
    if not changes_is_object then
        return redis.error_reply('ERR_ARG_TYPE Changes argument must be a JSON object, not an array (final check)')
    end

    local current_json_str = redis.call('GET', key)
    local current_doc

    if not current_json_str then
        -- Key doesn't exist, so the changes_doc becomes the new document
        current_doc = changes_doc
    else
        local err_current
        current_doc, err_current = cjson.decode(current_json_str)
        if not current_doc then
            return redis.error_reply('ERR_DECODE_EXISTING Invalid JSON in existing key ' .. key .. ': ' .. (err_current or 'unknown error'))
        end
        if type(current_doc) ~= 'table' then
             return redis.error_reply('ERR_EXISTING_TYPE Existing value at key ' .. key .. ' is not a JSON object, cannot merge.')
        end

        local current_is_object = false
        if next(current_doc) == nil then current_is_object = true else
            for k, _ in pairs(current_doc) do
                if type(k) ~= 'number' then current_is_object = true; break; end
            end
            if not current_is_object then -- Still could be an array
                local max_idx = 0; local count = 0
                for k, _ in pairs(current_doc) do
                    if type(k) == 'number' and k >= 1 and math.floor(k) == k then
                        if k > max_idx then max_idx = k end; count = count + 1
                    else current_is_object = true; break; end
                end
                if not current_is_object and count > 0 and max_idx == count then
                     return redis.error_reply('ERR_EXISTING_TYPE Existing value at key ' .. key .. ' is a JSON array, cannot merge object fields.')
                elseif not current_is_object and count == 0 and max_idx == 0 then current_is_object = true
                elseif not current_is_object and count > 0 and max_idx ~= count then current_is_object = true; end
            end
        end
        if not current_is_object then
             return redis.error_reply('ERR_EXISTING_TYPE Existing value at key ' .. key .. ' is a JSON array, cannot merge object fields (final check).')
        end

        -- Perform the shallow merge
        for k, v in pairs(changes_doc) do
            current_doc[k] = v
        end
    end

    local new_doc_json_str, err_encode = cjson.encode(current_doc)
    if not new_doc_json_str then
        return redis.error_reply('ERR_ENCODE Failed to encode merged document: ' .. (err_encode or 'unknown error'))
    end

    redis.call('SET', key, new_doc_json_str)
    return 1 -- Success
)lua";

const std::string LuaScriptManager::JSON_OBJECT_KEYS_LUA = LUA_COMMON_HELPERS + R"lua(
-- Script for JSON.OBJKEYS
-- KEYS[1] - The key where the JSON document is stored
-- ARGV[1] - The path to the object within the JSON document. Defaults to root '$' if not provided or empty.

local key = KEYS[1]
local path_str = ARGV[1]

-- Get the JSON string from Redis
local current_json_str = redis.call('GET', key)
if not current_json_str then
    return nil -- Key not found
end

-- Decode the JSON string
local current_doc, err_decode = cjson.decode(current_json_str)
if not current_doc then
    return redis.error_reply('ERR_DECODE Failed to decode JSON for key ' .. key .. ': ' .. (err_decode or 'unknown error'))
end

local target_object = current_doc
if path_str ~= '$' and path_str ~= '' and path_str ~= nil then
    local path_segments = parse_path(path_str)
    if path_segments == nil or (type(path_segments) == 'table' and path_segments.err) then -- Check for error reply from parse_path
         return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str .. (path_segments.err or ''))
    end
    if #path_segments > 0 then -- Only try to get sub-path if segments exist
        target_object = get_value_at_path(current_doc, path_segments)
    end
end

-- Check if the target is a table (potential object or array)
if type(target_object) ~= 'table' then
    return nil -- Path does not lead to an object or array, or path not found
end

-- Check if it's an object (not an array).
-- A simple heuristic: an array will have numeric keys 1..N and its length #target_object will be N.
-- An empty table {} could be an empty object or an empty array. OBJKEYS on empty array is an error in RedisJSON.
-- OBJKEYS on empty object returns an empty array.
local is_array = true
local n = 0
local first_key = next(target_object) -- Check if table is empty

if first_key == nil then -- Empty table: {}
    is_array = false -- Treat empty table as an object for OBJKEYS, returning []
else
    for k,v in pairs(target_object) do
        n = n + 1
        if type(k) ~= 'number' or k < 1 or k > n then -- Crude check, but good enough for cjson decoded tables
            is_array = false
            break
        end
    end
    if is_array and #target_object ~= n then -- e.g. sparse array like {1='a', 3='c'}
        is_array = false
    end
end

if is_array then
    return nil -- Target is an array, not an object. RedisJSON JSON.OBJKEYS returns error here. Let's return nil.
end

-- Collect keys from the object
local keys_array = {}
for k, v in pairs(target_object) do
    table.insert(keys_array, tostring(k)) -- Ensure keys are strings
end

-- Return the keys as a JSON array string
-- If keys_array is empty, cjson.encode({}) might produce "{}" (empty object)
-- which is not what we want. We need "[]" (empty array).
if #keys_array == 0 then
    return "[]"
else
    return cjson.encode(keys_array)
end
)lua";

const std::string LuaScriptManager::JSON_NUMINCRBY_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local increment_by_str = ARGV[2]

    if path_str == '$' or path_str == '' then
        return redis.error_reply('ERR_PATH path cannot be root for NUMINCRBY')
    end

    local current_json_str = redis.call('GET', key)
    if not current_json_str then
        return redis.error_reply('ERR_NOKEY key ' .. key .. ' does not exist')
    end

    local current_doc, err_decode = cjson.decode(current_json_str)
    if not current_doc then
        return redis.error_reply('ERR_DECODE Failed to decode JSON for key ' .. key .. ': ' .. (err_decode or 'unknown error'))
    end

    local path_segments = parse_path(path_str)
    if path_segments == nil or (type(path_segments) == 'table' and path_segments.err) then
         return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str .. (path_segments.err or ''))
    end
    if #path_segments == 0 then -- Should be caught by root check above, but as safeguard
        return redis.error_reply('ERR_PATH path cannot be root for NUMINCRBY (safeguard)')
    end

    local current_value = get_value_at_path(current_doc, path_segments)

    if current_value == nil then
        return redis.error_reply('ERR_NOPATH path ' .. path_str .. ' does not exist or is null')
    end

    if type(current_value) ~= 'number' then
        return redis.error_reply('ERR_TYPE value at path ' .. path_str .. ' is not a number, it is a ' .. type(current_value))
    end

    local increment_by = tonumber(increment_by_str)
    if increment_by == nil then
        return redis.error_reply('ERR_ARG_CONVERT increment value ' .. increment_by_str .. ' is not a valid number')
    end

    local new_value = current_value + increment_by

    -- Ensure the new value is a valid number for JSON (e.g. not NaN or Infinity)
    if new_value ~= new_value or new_value == math.huge or new_value == -math.huge then
        return redis.error_reply('ERR_OVERFLOW numeric overflow or invalid result after increment')
    end

    local success, err_set = set_value_at_path(current_doc, path_segments, new_value, false) -- create_path is false
    if not success then
        return redis.error_reply('ERR_SET_PATH Failed to set new numeric value: ' .. (err_set or 'unknown error'))
    end

    local new_doc_json_str, err_encode = cjson.encode(current_doc)
    if not new_doc_json_str then
        return redis.error_reply('ERR_ENCODE Failed to encode document after NUMINCRBY: ' .. (err_encode or 'unknown error'))
    end

    redis.call('SET', key, new_doc_json_str)

    return cjson.encode(new_value) -- Return the new value, JSON encoded
)lua";

const std::string LuaScriptManager::JSON_OBJECT_LENGTH_LUA = LUA_COMMON_HELPERS + R"lua(
-- Script for JSON.OBJLEN (emulated)
-- KEYS[1] - The key where the JSON document is stored
-- ARGV[1] - The path to the object within the JSON document. Defaults to root '$' if not provided or empty.

local key = KEYS[1]
local path_str = ARGV[1]

-- Get the JSON string from Redis
local current_json_str = redis.call('GET', key)
if not current_json_str then
    return nil -- Key not found, return nil (RedisJSON behavior for JSON.OBJLEN on non-existent key)
end

-- Decode the JSON string
local current_doc, err_decode = cjson.decode(current_json_str)
if not current_doc then
    return redis.error_reply('ERR_DECODE Failed to decode JSON for key ' .. key .. ': ' .. (err_decode or 'unknown error'))
end

local target_value = current_doc
if path_str ~= '$' and path_str ~= '' and path_str ~= nil then
    local path_segments = parse_path(path_str)
    if path_segments == nil or (type(path_segments) == 'table' and path_segments.err) then
         return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str .. (path_segments.err or ''))
    end
    if #path_segments > 0 then
        target_value = get_value_at_path(current_doc, path_segments)
    end
end

-- Check if the target value exists at the path
if target_value == nil then
    return nil -- Path does not lead to a value, return nil (RedisJSON behavior)
end

-- Check if the target is a table (potential object or array)
if type(target_value) ~= 'table' then
    return redis.error_reply('ERR_TYPE Path value is not an object or array, it is a ' .. type(target_value))
end

-- Check if it's an object (not an array).
-- Similar heuristic to JSON.OBJKEYS: an array has numeric keys 1..N and its length #target_value is N.
-- An empty table {} could be an empty object or an empty array. OBJLEN on empty array is an error in RedisJSON.
-- OBJLEN on empty object returns 0.
local is_array = true
local n = 0
local first_key = next(target_value)

if first_key == nil then -- Empty table: {}
    is_array = false -- Treat empty table as an object for OBJLEN, returning 0
else
    for k,v in pairs(target_value) do
        n = n + 1
        -- A robust check for array: all keys must be numbers, sequential, starting from 1.
        -- cjson decodes JSON arrays into Lua tables with integer keys 1..N.
        -- cjson decodes JSON objects into Lua tables with string keys (or mixed if original object had numeric strings as keys).
        if type(k) ~= 'number' then
            is_array = false
            break
        end
    end
    if is_array then -- All keys were numbers
        if #target_value ~= n then -- Check for sparseness or non-sequential keys if all were numbers
            is_array = false -- e.g. {1='a', 3='c'} is an object-like table, not a dense array
        end
    end
end

if is_array then
    -- RedisJSON v2.0.x JSON.OBJLEN on an array returns: "ERR element at path is not an object"
    -- RedisJSON v2.4.x+ JSON.OBJLEN on an array might return nil or specific error.
    -- Let's be consistent with error for non-object.
    return redis.error_reply('ERR_TYPE Path value is an array, not an object')
end

-- Count keys in the object
local key_count = 0
for _ in pairs(target_value) do
    key_count = key_count + 1
end

return key_count
)lua";

const std::string LuaScriptManager::JSON_ARRAY_INSERT_LUA = LUA_COMMON_HELPERS + R"lua(
    local key = KEYS[1]
    local path_str = ARGV[1]
    local index_str = ARGV[2]
    -- ARGV[3] onwards are the values to insert

    if #ARGV < 3 then
        return redis.error_reply('ERR_ARG_COUNT Not enough arguments for JSON.ARRINSERT')
    end

    local current_json_str = redis.call('GET', key)
    if not current_json_str then
        return redis.error_reply('ERR_NOKEY Key ' .. key .. ' does not exist')
    end

    local doc, err_decode = cjson.decode(current_json_str)
    if not doc then
        return redis.error_reply('ERR_DECODE Failed to decode JSON for key ' .. key .. ': ' .. (err_decode or 'unknown error'))
    end

    local target_array_ref = doc
    if path_str ~= '$' and path_str ~= '' then
        local path_segments = parse_path(path_str)
        if path_segments == nil or (type(path_segments) == 'table' and path_segments.err) then
             return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str .. (path_segments.err or ''))
        end
        if #path_segments > 0 then
            target_array_ref = get_value_at_path(doc, path_segments)
        end
    end

    if target_array_ref == nil then
        return redis.error_reply('ERR_NOPATH Path ' .. path_str .. ' does not exist or is null')
    end

    if type(target_array_ref) ~= 'table' then
        return redis.error_reply('ERR_NOT_ARRAY Path ' .. path_str .. ' does not point to an array (type: ' .. type(target_array_ref) .. ')')
    end

    -- Validate if it's actually array-like (all numeric keys 1..N)
    local is_potential_array = true
    local n_elements = 0
    for k, _ in pairs(target_array_ref) do
        if type(k) ~= 'number' then is_potential_array = false; break; end
        n_elements = n_elements + 1
    end
    if is_potential_array and #target_array_ref ~= n_elements and n_elements > 0 then
        -- It has numeric keys but might be sparse, or #target_array_ref is 0 for table like {["1"]=val}
        -- A stricter check might be needed, but for cjson decoded arrays, #target_array_ref should be reliable.
        -- If it has non-sequential numeric keys, table.insert might behave unexpectedly or create sparse array.
        -- For simplicity, we rely on #target_array_ref for length of dense arrays.
        -- If cjson can decode {"0":"a", "1":"b"} into a Lua table where # gives 0, this check needs refinement.
        -- Standard cjson behavior for JSON arrays `["a","b"]` is Lua table `{ "a", "b" }` where # is 2.
    end
    -- No, the above check is not perfect. A simple `type(target_array_ref) == 'table'` is used by other array ops.
    -- Let's assume if path leads to a JSON object, it's an error. If it leads to JSON array, it's a Lua table.
    -- We need a better way to distinguish Lua table-as-object from table-as-array.
    -- For now, rely on `get_value_at_path` returning a table and proceed.
    -- RedisJSON's error for non-array: "ERR element at path is not an array"

    local insert_idx = tonumber(index_str)
    if insert_idx == nil then
        return redis.error_reply('ERR_INDEX Invalid index: ' .. index_str .. ' is not a number')
    end

    local arr_len = #target_array_ref

    -- Convert 0-based (client) or negative index to 1-based Lua index
    if insert_idx == 0 then -- Insert at the beginning
        insert_idx = 1
    elseif insert_idx > 0 then -- Positive index
        insert_idx = insert_idx + 1 -- Convert 0-based client to 1-based Lua for position *after* which to insert
                                    -- No, table.insert(list, pos, val) inserts AT pos, shifting others.
                                    -- So, 0-based client index 'i' becomes 1-based Lua index 'i+1'.
        if insert_idx > arr_len + 1 then -- If index is out of bounds (too large)
            insert_idx = arr_len + 1 -- Append to the end
        end
    else -- Negative index (insert_idx < 0)
        -- -1 means insert before the last element (i.e. at current len)
        -- -2 means insert before the second to last (i.e. at current len - 1)
        -- So, effective_idx = arr_len + insert_idx + 1 (in 1-based)
        insert_idx = arr_len + insert_idx + 1
        if insert_idx < 1 then -- If index is too small (e.g., -ve larger than arr_len)
            insert_idx = 1 -- Insert at the beginning
        end
    end

    -- If after adjustments, index is still problematic (e.g. target_array_ref was empty, arr_len=0)
    -- For an empty array, arr_len = 0.
    -- If insert_idx (client) = 0, Lua idx = 1. Correct.
    -- If insert_idx (client) = -1, Lua idx = 0 + (-1) + 1 = 0. This should become 1.
    -- Let's refine negative and boundary conditions for Lua's table.insert:
    -- Lua's table.insert(t, pos, val) inserts val at position pos.
    -- If pos > #t + 1, it's an error by default Lua, but RedisJSON appends.
    -- If pos < 1, it's an error. RedisJSON prepends.

    if arr_len == 0 then -- Target array is empty
        insert_idx = 1 -- Always insert at the beginning for an empty array
    else
        if index_str == "0" then -- Original client index was 0
            insert_idx = 1
        else
            local client_idx = tonumber(index_str) -- Re-parse for clarity
            if client_idx > 0 then
                insert_idx = client_idx + 1
                if insert_idx > arr_len + 1 then insert_idx = arr_len + 1 end
            elseif client_idx < 0 then
                insert_idx = arr_len + client_idx + 1
                if insert_idx < 1 then insert_idx = 1 end
            else -- client_idx == 0 was handled by index_str == "0"
                 -- This path should not be taken if index_str is a valid number != 0
            end
        end
    end


    local values_to_insert = {}
    for i = 3, #ARGV do
        local val_json_str = ARGV[i]
        local val -- This will hold the decoded value if successful
        local err_msg -- This will hold the error message if decode fails

        local success, result = pcall(cjson.decode, val_json_str)

        if success then
            val = result
            -- If cjson.decode successfully returns Lua nil for an input that wasn't the string "null",
            -- (e.g. if cjson.decode("") -> nil), treat it as a failure for consistency with original script's expectation.
            -- Note: cjson.decode("null") results in `cjson.null`, not Lua `nil`.
            -- `val == nil` checks for actual Lua nil.
            if val == nil and val_json_str ~= "null" then
                -- This specific path might indicate an unexpected behavior from cjson.decode for certain inputs (e.g. empty string)
                -- if it returns Lua nil instead of erroring or returning a cjson.null or other value.
                -- Standard cjson would typically error on empty string or malformed JSON.
                err_msg = "Input string decoded to Lua nil, but input was not 'null'"
                success = false -- Force error path below
            end
        else
            -- pcall failed, result contains the error message from cjson.decode
            err_msg = tostring(result) -- Ensure it's a string
        end

        -- Check 'success' flag (from pcall, or overridden if decoded to nil for non-null input)
        if not success then
            return redis.error_reply('ERR_DECODE_ARG Failed to decode value argument #' .. (i-2) .. ' ("' .. val_json_str .. '"): ' .. (err_msg or 'unknown decode error'))
        end

        table.insert(values_to_insert, val)
    end

    if #values_to_insert == 0 then
        return redis.error_reply('ERR_NO_VALUES No values provided for insertion')
    end

    -- Insert values one by one. Each subsequent insert will be at an incremented index
    -- due to previous insertions shifting elements.
    for i, value_to_insert in ipairs(values_to_insert) do
        table.insert(target_array_ref, insert_idx, value_to_insert)
        insert_idx = insert_idx + 1 -- Next value goes after the one just inserted
    end

    local new_doc_json_str, err_encode = cjson.encode(doc)
    if not new_doc_json_str then
        return redis.error_reply('ERR_ENCODE Failed to encode document after array insert: ' .. (err_encode or 'unknown error'))
    end

    redis.call('SET', key, new_doc_json_str)

    return #target_array_ref -- Return the new length of the array
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
    // std::cout << "LOG: LuaScriptManager::load_script() - Entry for script name: '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    if (name.empty() || script_body.empty()) {
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - Script name or body is empty. Name: '" << name << "'. Throwing." << std::endl;
        throw std::invalid_argument("Script name and body cannot be empty.");
    }

    // std::cout << "LOG: LuaScriptManager::load_script() - Getting connection for SCRIPT LOAD '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    RedisConnectionManager::RedisConnectionPtr conn_guard = connection_manager_->get_connection();
    RedisConnection* conn = conn_guard.get();

    if (!conn || !conn->is_connected()) {
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - Failed to get valid Redis connection for SCRIPT LOAD '" << name << "'. Conn is " << (conn ? "not null but not connected" : "null") << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection if it's not null
        throw ConnectionException("Failed to get valid Redis connection for SCRIPT LOAD.");
    }
    // std::cout << "LOG: LuaScriptManager::load_script() - Got connection for SCRIPT LOAD '" << name << "'. Host: " << conn->get_host() << ":" << conn->get_port() << ". Thread ID: " << std::this_thread::get_id() << std::endl;

    // std::cout << "LOG: LuaScriptManager::load_script() - Calling conn->command('SCRIPT LOAD ...') for script '" << name << "'. First 30 chars of body: " << script_body.substr(0, 30) << (script_body.length() > 30 ? "..." : "") << ". Thread ID: " << std::this_thread::get_id() << std::endl;
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
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - conn->command('SCRIPT LOAD') for '" << name << "' returned null reply. Error context:" << error_context_for_throw << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "No reply from Redis for script '" + name + "'" + error_context_for_throw);
    }
    // std::cout << "LOG: LuaScriptManager::load_script() - conn->command('SCRIPT LOAD') for '" << name << "' returned. Reply type: " << reply->type << ". Thread ID: " << std::this_thread::get_id() << std::endl;

    std::string sha1_hash;
    if (reply->type == REDIS_REPLY_STRING) {
        sha1_hash = std::string(reply->str, reply->len);
        // std::cout << "LOG: LuaScriptManager::load_script() - Script '" << name << "' loaded successfully. SHA1: " << sha1_hash << ". Thread ID: " << std::this_thread::get_id() << std::endl;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        std::string err_msg = (reply->str ? std::string(reply->str, reply->len) : "Unknown Redis error");
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' failed with REDIS_REPLY_ERROR: " << err_msg << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "Error for script '" + name + "': " + err_msg);
    } else {
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' returned unexpected reply type: " << reply->type << ". Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
        // conn_guard will release connection
        throw RedisCommandException("SCRIPT LOAD", "Unexpected reply type for script '" + name + "': " + std::to_string(reply->type));
    }

    if (sha1_hash.empty()) {
        // std::cout << "ERROR_LOG: LuaScriptManager::load_script() - SCRIPT LOAD for '" << name << "' resulted in empty SHA1 hash. Throwing. Thread ID: " << std::this_thread::get_id() << std::endl;
         // conn_guard will release connection
         throw RedisCommandException("SCRIPT LOAD", "Failed to load script '" + name + "', SHA1 hash is empty.");
    }

    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        script_shas_[name] = sha1_hash;
        // std::cout << "LOG: LuaScriptManager::load_script() - Cached SHA1 for script '" << name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    }

    // Connection is returned by RedisConnectionPtr conn_guard going out of scope.
    // No explicit call to return_connection needed here for logging its return.
    // std::cout << "LOG: LuaScriptManager::load_script() - Exiting for script name: '" << name << "'. Connection will be returned by RAII. Thread ID: " << std::this_thread::get_id() << std::endl;
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
    {"json_get_set", &LuaScriptManager::ATOMIC_JSON_GET_SET_PATH_LUA}, // Renamed from atomic_json_get_set_path
    {"json_compare_set", &LuaScriptManager::ATOMIC_JSON_COMPARE_SET_PATH_LUA}, // Renamed from atomic_json_compare_set_path
    {"json_sparse_merge", &LuaScriptManager::JSON_SPARSE_MERGE_LUA},
    {"json_object_keys", &LuaScriptManager::JSON_OBJECT_KEYS_LUA},
    {"json_numincrby", &LuaScriptManager::JSON_NUMINCRBY_LUA},
    {"json_object_length", &LuaScriptManager::JSON_OBJECT_LENGTH_LUA},
    {"json_array_insert", &LuaScriptManager::JSON_ARRAY_INSERT_LUA},
    {"json_clear", &LuaScriptManager::JSON_CLEAR_LUA}
    // Add any other scripts here if they are defined as static const std::string members
};

const std::string LuaScriptManager::JSON_CLEAR_LUA = LUA_COMMON_HELPERS + R"lua(
-- Refined JSON_CLEAR_LUA (Attempt 3 to fix test failures)
local function do_clear_recursive(target_value, is_target_array_hint)
    local count = 0
    local actually_modified_structure = false

    if type(target_value) ~= 'table' then
        return 0, false
    end

    local is_array = is_target_array_hint
    if is_array == nil then
        is_array = true; local n = 0
        if next(target_value) == nil then is_array = true; else
            for k, _ in pairs(target_value) do
                n = n + 1
                if type(k) ~= 'number' or k < 1 or k > n then is_array = false; break; end
            end
            if is_array and #target_value ~= n then is_array = false; end
        end
    end

    if is_array then
        if #target_value > 0 then
            for i = #target_value, 1, -1 do
                table.remove(target_value, i) -- Modifies target_value in place
            end
            count = 1
            actually_modified_structure = true
            -- No need to replace with cjson.decode('[]') here, as table.remove modifies in place.
            -- The issue is when this modified (now empty) table is assigned back to its parent if it's a sub-element.
            -- Or if target_value itself is the root.
        else
            count = 0 -- Already empty
        end
    else -- It's an object
        local keys_to_iterate = {}
        for k, _ in pairs(target_value) do table.insert(keys_to_iterate, k) end

        for _, k in ipairs(keys_to_iterate) do
            local v = target_value[k]
            local item_modified_this_iteration = false

            if type(v) == 'number' then
                if target_value[k] ~= 0 then
                    target_value[k] = 0
                    count = count + 1
                    item_modified_this_iteration = true
                end
            elseif type(v) == 'table' then
                local sub_is_array_hint = nil; local sub_n_keys = 0;
                if next(v) == nil then sub_is_array_hint = true; else
                    for sk_sub,_ in pairs(v) do sub_n_keys = sub_n_keys+1; if type(sk_sub)~='number' or sk_sub<1 or sk_sub>sub_n_keys then sub_is_array_hint=false; break; end end
                    if sub_is_array_hint == nil and #v ~= sub_n_keys then sub_is_array_hint = false; end
                    if sub_is_array_hint == nil then sub_is_array_hint = true; end
                end

                local sub_cleared_count, sub_modified = do_clear_recursive(v, sub_is_array_hint)

                if sub_cleared_count > 0 then count = count + sub_cleared_count; end
                if sub_modified then item_modified_this_iteration = true; end

                -- If the sub-table was an array and it's now empty due to clearing,
                -- replace it with a proper empty array Lua table.
                if sub_is_array_hint and #v == 0 and sub_modified then -- sub_modified ensures it became empty *due to clearing*
                    target_value[k] = cjson.decode('[]') -- Replace with a table cjson knows is an array
                end
            end
            if item_modified_this_iteration then actually_modified_structure = true; end
        end
    end
    return count, actually_modified_structure
end

local key = KEYS[1]
local path_str = ARGV[1]
if path_str == nil then path_str = '$' end

local current_json_str = redis.call('GET', key)

if not current_json_str then
    if path_str == '$' or path_str == '' then return 0; end
    return redis.error_reply('ERR document not found')
end

local current_doc, err_decode = cjson.decode(current_json_str)
if not current_doc then
    return redis.error_reply('ERR_DECODE Failed to decode JSON for key ' .. key .. ': ' .. (err_decode or 'unknown error'))
end

local cleared_count = 0
local doc_modified_overall = false -- Renamed for clarity

if path_str == '$' or path_str == '' then
    if type(current_doc) == 'table' then
        local root_is_array = nil; local n_root_keys=0;
        if next(current_doc)==nil then root_is_array=true; else
            for k_root,_ in pairs(current_doc) do n_root_keys=n_root_keys+1; if type(k_root)~='number' or k_root<1 or k_root>n_root_keys then root_is_array=false; break; end end
            if root_is_array==nil and #current_doc~=n_root_keys then root_is_array=false; end
            if root_is_array==nil then root_is_array=true; end
        end
        cleared_count, doc_modified_overall = do_clear_recursive(current_doc, root_is_array)
        if root_is_array and #current_doc == 0 and doc_modified_overall then
             -- If root was an array and is now empty and was modified (cleared)
             current_doc = cjson.decode('[]')
        end
    else
        cleared_count = 1
        -- doc_modified_overall remains false for scalar root
    end
else -- Path is not root
    local path_segments = parse_path(path_str)
    if path_segments == nil or (type(path_segments) == 'table' and path_segments.err) then
        return redis.error_reply('ERR_PATH Invalid path string: ' .. path_str .. (path_segments.err or ''))
    end

    if #path_segments == 0 then -- Should be caught by root check
         if type(current_doc) == 'table' then
            local root_is_array_s = nil; local n_s_keys=0; if next(current_doc)==nil then root_is_array_s=true; else for ks,_ in pairs(current_doc) do n_s_keys=n_s_keys+1; if type(ks)~='number' or ks<1 or ks>n_s_keys then root_is_array_s=false;break;end;end;if root_is_array_s==nil and #current_doc ~= n_s_keys then root_is_array_s=false;end;if root_is_array_s==nil then root_is_array_s=true;end end
            cleared_count, doc_modified_overall = do_clear_recursive(current_doc, root_is_array_s)
            if root_is_array_s and #current_doc == 0 and doc_modified_overall then
                 current_doc = cjson.decode('[]')
            end
        else cleared_count = 1; end
    else
        -- Traverse to the parent to get a reference to the target_value's container
        local parent_ref = current_doc
        for i = 1, #path_segments - 1 do
            local segment = path_segments[i]
            if type(parent_ref) ~= 'table' or parent_ref[segment] == nil then return 0; end -- Path not found
            parent_ref = parent_ref[segment]
        end

        local final_segment = path_segments[#path_segments]
        if type(parent_ref) ~= 'table' or parent_ref[final_segment] == nil then return 0; end -- Path not found

        local target_value_ref = parent_ref[final_segment]

        if type(target_value_ref) == 'table' then
            local target_is_array_hint = nil; local n_target_keys=0; if next(target_value_ref)==nil then target_is_array_hint=true; else for kt,_ in pairs(target_value_ref) do n_target_keys=n_target_keys+1; if type(kt)~='number' or kt<1 or kt>n_target_keys then target_is_array_hint=false;break;end;end;if target_is_array_hint==nil and #target_value_ref ~= n_target_keys then target_is_array_hint=false;end;if target_is_array_hint==nil then target_is_array_hint=true;end end

            local sub_cleared_count, sub_modified
            sub_cleared_count, sub_modified = do_clear_recursive(target_value_ref, target_is_array_hint)
            cleared_count = sub_cleared_count

            if sub_modified then
                doc_modified_overall = true
                if target_is_array_hint and #target_value_ref == 0 then
                    parent_ref[final_segment] = cjson.decode('[]') -- Replace in parent
                end
            end
        else
            cleared_count = 1
            -- doc_modified_overall remains false for scalar target
        end
    end
end

if doc_modified_overall then
    local new_doc_json_str, err_encode = cjson.encode(current_doc)
    if not new_doc_json_str then
        return redis.error_reply('ERR_ENCODE Failed to encode document: ' .. (err_encode or 'unknown error'))
    end
    redis.call('SET', key, new_doc_json_str)
end

return cleared_count
)lua";

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

        // std::cerr << "INFO: Lua script '" << name << "' not found in cache. Attempting on-demand load." << std::endl;
        attempted_on_demand_load = true; // Mark that we are trying/tried
        const std::string* script_body_ptr = get_script_body_by_name(name);

        if (!script_body_ptr) {
            throw LuaScriptException(name, "Script body not found for on-demand loading of script: " + name);
        }

        try {
            load_script(name, *script_body_ptr); // This will populate script_shas_ if successful
            // std::cerr << "INFO: Successfully loaded Lua script '" << name << "' on demand." << std::endl;
            // Loop will continue to re-acquire lock and find SHA.
        } catch (const RedisJSONException& e) {
            // std::cerr << "ERROR: Failed to load Lua script '" << name << "' on demand: " << e.what() << std::endl;
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
    // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Entry. Thread ID: " << std::this_thread::get_id() << std::endl;
    int success_count = 0;
    int fail_count = 0;
    int script_idx = 0;

    for (const auto& pair : SCRIPT_DEFINITIONS) {
        script_idx++;
        const std::string& script_name = pair.first;
        const std::string* script_body = pair.second;
        // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Attempting to load script " << script_idx << "/" << SCRIPT_DEFINITIONS.size() << ": '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
        try {
            load_script(script_name, *script_body);
            // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Successfully preloaded script: '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
            success_count++;
        } catch (const RedisJSONException& e) {
            fail_count++;
            // Using std::cerr for warnings/errors is conventional, but sticking to cout if all logs go there.
            // For now, commenting out as per instruction. If actual logging framework is used, this would be a proper log_warning.
            // std::cerr << "WARNING: LuaScriptManager::preload_builtin_scripts() - Failed to preload Lua script '" << script_name << "' (RedisJSONException): " << e.what() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
        } catch (const std::exception& e) {
            fail_count++;
            // std::cerr << "WARNING: LuaScriptManager::preload_builtin_scripts() - Unexpected std::exception while preloading Lua script '" << script_name << "': " << e.what() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
        }
        #ifndef NDEBUG
        catch (...) {
            fail_count++;
            // std::cerr << "WARNING: LuaScriptManager::preload_builtin_scripts() - Unknown error while preloading Lua script '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
        }
        #endif
        // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Finished attempt for script '" << script_name << "'. Thread ID: " << std::this_thread::get_id() << std::endl;
    }

    if (fail_count > 0) {
        // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Preloading completed. Success: " << success_count
        //           << ", Failed: " << fail_count << " (out of " << SCRIPT_DEFINITIONS.size() << " total). Thread ID: " << std::this_thread::get_id() << std::endl;
    } else {
        // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Successfully preloaded all " << success_count << " built-in Lua scripts. Thread ID: " << std::this_thread::get_id() << std::endl;
    }
    // std::cout << "LOG: LuaScriptManager::preload_builtin_scripts() - Exit. Thread ID: " << std::this_thread::get_id() << std::endl;
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
