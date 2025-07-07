# RedisJSON++ Lua Scripts Design

## 1. Introduction

RedisJSON++ leverages Lua scripting to perform complex JSON operations directly on the Redis server. Since standard Redis stores data as strings, manipulating JSON structures (which are stored as serialized strings) often involves multiple steps: fetching the string, deserializing it, modifying it, serializing it back, and storing it. Performing these steps client-side would require multiple network round-trips and would not be atomic.

Lua scripts executed by Redis run atomically. This means that a script executes from start to finish without interruption from other commands. By encapsulating the entire read-modify-write cycle for JSON documents within a single Lua script, RedisJSON++ achieves atomicity for its JSON operations. This approach avoids the need for the dedicated RedisJSON module, allowing users to work with JSON data on any standard Redis instance (version 2.6 or higher, which introduced Lua scripting, though Redis 5.0+ is generally recommended for performance and features like `cjson`).

The scripts typically perform the following sequence:
1.  Fetch the JSON string from a given Redis key.
2.  Decode the JSON string into a Lua table using the `cjson` library (available in Redis's Lua environment).
3.  Perform the requested operation (e.g., get a value at a path, set a value, delete a part of the JSON, append to an array).
4.  Encode the modified Lua table back into a JSON string.
5.  Store the new JSON string back into the Redis key.
6.  Return a result indicating success, failure, or the requested data.

This document details the common helper functions used across these scripts and then breaks down the functionality of each specific Lua script responsible for JSON CRUD (Create, Read, Update, Delete) and other specialized operations.

## 2. Common Lua Helper Functions

Several Lua helper functions are prepended to most of the main operational scripts. These functions provide common utilities for path parsing and value manipulation within a Lua table representation of a JSON document.

### 2.1 `parse_path(path_str)`

*   **Purpose**: Converts a JSON path string (e.g., `$.obj.arr[0].field` or `obj.arr[0]`) into a Lua table of segments that can be used to traverse the JSON structure.
*   **Input**: `path_str` (string) - The JSON path.
*   **Output**: A Lua table (array) of path segments. Object keys are strings, and array indices are numbers (1-based for Lua compatibility). Returns an empty table for root paths (`$`, `''`, or `nil`). Can return `redis.error_reply` for malformed paths.
*   **Logic**:
    1.  Handles root path: If `path_str` is `nil`, `$`, or an empty string, it returns an empty table, signifying the root of the JSON document.
    2.  Normalizes path: Removes leading `$.` or `$[`, converting paths like `$.a.b` to `a.b` and `$[0]` to `[0]` for simpler parsing.
    3.  Iterative Parsing: The function iterates through the `path_str` character by character.
        *   **Object Keys**: It matches sequences of characters that are not `.` or `[` as object keys. These are added to the `segments` table as strings.
        *   **Array Indices**: When a `[` is encountered, it extracts the content between `[` and `]` as an index.
            *   This index string is converted to a number using `tonumber()`.
            *   If the conversion fails (non-numeric index), it returns an error.
            *   The numeric index is incremented by 1 (e.g., JSON `[0]` becomes Lua `[1]`) and added to the `segments` table.
        *   **Path Separators**: It skips `.` characters.
    4.  Error Handling: If an unmatched `[` or other unexpected characters are found, it returns a Redis error reply indicating a malformed path.

### 2.2 `get_value_at_path(doc, path_segments)`

*   **Purpose**: Retrieves a value from a Lua table (representing a decoded JSON document) based on a pre-parsed list of path segments.
*   **Input**:
    *   `doc` (table) - The Lua table representing the JSON document.
    *   `path_segments` (table) - The array of path segments obtained from `parse_path`.
*   **Output**: The value found at the specified path, or `nil` if the path does not exist or leads into a non-table intermediate value.
*   **Logic**:
    1.  Initialization: `current` is set to the input `doc`.
    2.  Traversal: It iterates through each `segment` in `path_segments`.
        *   If `current` is not a table at any point during traversal (and more segments remain), it means the path is invalid (e.g., trying to access `a.b` where `a` is a string), so it returns `nil`.
        *   `current` is updated to `current[segment]`.
        *   If `current` becomes `nil` at any step, it means the specific key or index does not exist, so the function returns `nil`.
    3.  Return Value: After successfully traversing all segments, the final `current` value is returned.

### 2.3 `set_value_at_path(doc, path_segments, value_to_set, create_path_flag)`

*   **Purpose**: Sets a value within a Lua table (JSON document) at a location specified by path segments. It can optionally create intermediate objects/arrays if they don't exist.
*   **Input**:
    *   `doc` (table) - The Lua table representing the JSON document.
    *   `path_segments` (table) - The array of path segments.
    *   `value_to_set` (any) - The Lua value to set at the target path.
    *   `create_path_flag` (boolean) - If `true`, missing intermediate objects or arrays in the path will be created. If `false`, the operation fails if any intermediate path does not exist.
*   **Output**: A Lua table: `{success (boolean), message (string)}`. `success` is `true` if the value was set, `false` otherwise. `message` provides details on success ("OK") or failure.
*   **Logic**:
    1.  Initialization: `current` is set to `doc`.
    2.  Intermediate Path Traversal: It iterates from the first segment up to the second-to-last segment (`#path_segments - 1`).
        *   If `current` is not a table, it returns `false` with an error message.
        *   If `current[segment]` is `nil` (path segment doesn't exist) or is not a table:
            *   If `create_path_flag` is `true`: It creates a new table (object `{}` or array `{}`) at `current[segment]`. It determines whether to create an object or array based on the type of the *next* segment (if the next segment is a number, it implies the current segment should hold an array, otherwise an object. This logic is simplified in the current implementation to always create `{}`).
            *   If `create_path_flag` is `false`: It returns `false` with an error message.
        *   `current` is updated to `current[segment]`.
    3.  Set Final Value:
        *   The `final_segment` is `path_segments[#path_segments]`.
        *   If `current` (which now refers to the parent of the target location) is not a table, it returns `false`.
        *   The value is set: `current[final_segment] = value_to_set`.
    4.  Return Success: Returns `{true, "OK"}`.

### 2.4 `del_value_at_path(doc, path_segments)`

*   **Purpose**: Deletes a key-value pair from an object or an element from an array within a Lua table (JSON document) at a location specified by path segments.
*   **Input**:
    *   `doc` (table) - The Lua table representing the JSON document.
    *   `path_segments` (table) - The array of path segments.
*   **Output**: A Lua table: `{success (boolean), message (string)}`. `success` is `true` if deletion occurred or if the path didn't exist (idempotency). `message` is "OK" for successful deletion, a descriptive string if the item was already not found, or an error message if the path is invalid.
*   **Logic**:
    1.  Handle Root Deletion Attempt: If `path_segments` is empty (attempting to delete the root), it returns `{false, "Cannot delete root..."}`. Deleting the entire document should be done via `DEL key`.
    2.  Intermediate Path Traversal: Similar to `set_value_at_path`, it iterates up to the parent of the target element.
        *   If an intermediate segment is not a table, it returns `{false, "Path segment ... is not a table/array"}`.
        *   If `current[segment]` becomes `nil`, it means the path doesn't fully exist. Deletion is considered successful in an idempotent way, returning `{true, "Intermediate path segment ... not found, nothing to delete"}`.
        *   `current` is updated to `current[segment]`.
    3.  Delete Final Value:
        *   The `final_segment` is `path_segments[#path_segments]`.
        *   If `current` (parent of the target) is a table:
            *   If `current[final_segment]` exists (is not `nil`), it's set to `nil` (Lua's way of deleting a table key). Returns `{true, "OK"}`.
            *   If `current[final_segment]` is already `nil`, it returns `{true, "Final path segment ... not found, nothing to delete"}`.
        *   If `current` is not a table, it returns `{false, "Final path leads to a non-table parent..."}`.

## 3. CRUD-Related Lua Scripts

The following sections describe the main Lua scripts responsible for JSON Create, Read, Update, and Delete (CRUD) operations, as well as other specialized atomic operations. All these scripts utilize the common helper functions described above.

Each script generally follows this pattern:
1.  Retrieve the existing JSON string from Redis using `redis.call('GET', key)`.
2.  Handle cases where the key might not exist.
3.  Decode the JSON string to a Lua table using `cjson.decode()`. Handle decoding errors.
4.  Parse the input path string using `parse_path()` if applicable.
5.  Perform the core logic (get, set, delete, append, etc.) using the helper functions or direct Lua table manipulations.
6.  Encode the (potentially modified) Lua table back to a JSON string using `cjson.encode()`. Handle encoding errors.
7.  Write the new JSON string back to Redis using `redis.call('SET', key, new_json_string)`.
8.  Optionally set a TTL on the key using `redis.call('EXPIRE', key, ttl)`.
9.  Return an appropriate value (e.g., the retrieved data, status of the operation, new array length).

### 3.1 `JSON_PATH_GET_LUA` (Read)

*   **Purpose**: Retrieves a value from a JSON document stored in Redis, specified by a key and a JSONPath-like string.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key holding the JSON string.
    *   `ARGV[1]`: The JSON path string (e.g., `$.field`, `object.array[0]`).
*   **Logic Flow**:
    1.  Fetches the JSON string for `KEYS[1]`. If not found, returns `nil`.
    2.  Decodes the JSON string. If decoding fails, returns a Redis error.
    3.  If `path_str` is `$` or empty, it means the root. Encodes and returns the entire document.
    4.  Parses `ARGV[1]` (path string) using `parse_path`. If path parsing fails, returns a Redis error.
    5.  Retrieves the value at the parsed path using `get_value_at_path`.
    6.  If `value_at_path` is `nil` (path not found), returns an empty JSON array string (`"[]"`) as per some conventions (e.g., RedisJSON module often returns arrays for JSONPath queries).
    7.  Otherwise, encodes `value_at_path` (wrapped in an array `{value_at_path}`) as a JSON string and returns it.
*   **Returns**: JSON string of an array containing the value(s) at the path, an empty array string `[]` if path not found, the full JSON document if path is root, or `nil` if the key doesn't exist.

### 3.2 `JSON_PATH_SET_LUA` (Create/Update)

*   **Purpose**: Sets or updates a value within a JSON document at a specified path. Can create the document or path if it doesn't exist. Supports conditional setting (NX/XX).
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: The JSON path string. `$` or empty string targets the root.
    *   `ARGV[2]`: The JSON string representation of the value to set.
    *   `ARGV[3]`: Condition for setting:
        *   `NX`: Set only if the path does not exist.
        *   `XX`: Set only if the path already exists.
        *   (empty/other): Set unconditionally.
    *   `ARGV[4]`: TTL (Time To Live) for the key, in seconds. If `0` or not a positive number, TTL is not set.
    *   `ARGV[5]`: `create_path_flag` (string "true" or "false") - whether to create intermediate paths if they don't exist during set.
*   **Logic Flow**:
    1.  Retrieves `current_json_str` for `KEYS[1]`.
    2.  Determines `path_exists`:
        *   If `current_json_str` exists, decodes it. If path is root, `path_exists` is true. Otherwise, parses path and uses `get_value_at_path` to check.
        *   If `current_json_str` does not exist: `path_exists` is false. If condition is `XX`, returns `false`. Initializes `current_doc` as an empty table `{}`. If path is root, `path_exists` is considered true for a new document.
    3.  Applies `NX`/`XX` conditions: If `condition == 'NX'` and `path_exists` is true, returns `false`. If `condition == 'XX'` and `path_exists` is false, returns `false`.
    4.  Decodes `ARGV[2]` (new value JSON string). If decoding fails, returns a Redis error.
    5.  If `path_str` is root (`$` or empty):
        *   Validates that `new_value` is a table (object/array) or `cjson.null`. If not, returns error.
        *   `current_doc` is replaced by `new_value`.
    6.  Else (path is not root):
        *   Parses `path_str`. If path parsing fails, returns a Redis error.
        *   Calls `set_value_at_path(current_doc, path_segments, new_value, create_path_flag)`. If it fails, returns a Redis error.
    7.  Encodes the modified `current_doc` to `new_doc_json_str`. If encoding fails, returns a Redis error.
    8.  Sets `KEYS[1]` to `new_doc_json_str` using `redis.call('SET', ...)`.
    9.  If `ttl_str` (ARGV[4]) is a positive number, applies `EXPIRE` to `KEYS[1]`.
*   **Returns**: `true` on successful set, `false` if `NX`/`XX` condition not met. Redis error on failure.

### 3.3 `JSON_PATH_DEL_LUA` (Delete)

*   **Purpose**: Deletes a value from a JSON document at a specified path. If path is root (`$`), deletes the entire key.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: The JSON path string.
*   **Logic Flow**:
    1.  Fetches JSON string for `KEYS[1]`. If not found, returns `0` (nothing deleted).
    2.  Decodes JSON string. If decoding fails, returns a Redis error.
    3.  If `path_str` is root (`$` or empty): Calls `redis.call('DEL', key)` and returns its result (number of keys deleted, typically 1 or 0).
    4.  Else (path is not root):
        *   Parses `path_str`. If path parsing fails, returns a Redis error.
        *   Calls `del_value_at_path(current_doc, path_segments)`.
        *   If `del_value_at_path` returns `success == false`, returns a Redis error.
        *   If `del_value_at_path` indicates nothing was deleted (e.g., path didn't exist, `msg ~= 'OK'`), returns `0`.
        *   Encodes the modified `current_doc`. If encoding fails, returns a Redis error.
        *   Sets `KEYS[1]` to the new JSON string.
*   **Returns**: `1` if deletion at path was successful, `0` if key or path did not exist. Result of `DEL` if root path. Redis error on failure.

### 3.4 `JSON_ARRAY_APPEND_LUA` (Create/Update)

*   **Purpose**: Appends one or more values to a JSON array at a specified path.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: Path to the array. If `$` or empty, targets the root if it's an array.
    *   `ARGV[2]`: JSON string of the value to append.
*   **Logic Flow**:
    1.  Fetches JSON string for `KEYS[1]`. If not found, returns `ERR_NOKEY`.
    2.  Decodes JSON string. If fails, returns `ERR_DECODE`.
    3.  Decodes `ARGV[2]` (value to append). If fails, returns `ERR_DECODE_ARG`.
    4.  Identifies `target_array_ref`:
        *   If `path_str` is root (`$` or empty), `target_array_ref` is the `doc` itself.
        *   Otherwise, parses `path_str` and uses `get_value_at_path` to get a reference to the array.
    5.  Validations:
        *   If `target_array_ref` is `nil` (path not found), returns `ERR_NOPATH`.
        *   If `type(target_array_ref)` is not 'table' (not an array/object), returns `ERR_NOT_ARRAY`. (Note: Lua tables are used for both objects and arrays; more robust checking for array-like structure might be needed if strictness is required, but `table.insert` works on any table).
    6.  Appends value: `table.insert(target_array_ref, value_to_append)`.
    7.  Encodes the modified `doc`. If fails, returns `ERR_ENCODE`.
    8.  Sets `KEYS[1]` to the new JSON string.
*   **Returns**: The new length of the array after appending. Redis error on failure.

### 3.5 `JSON_ARRAY_PREPEND_LUA` (Create/Update)

*   **Purpose**: Prepends one or more values to a JSON array at a specified path. (Functionally very similar to Append but uses `table.insert(target_array_ref, 1, value_to_prepend)`).
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: Path to the array.
    *   `ARGV[2]`: JSON string of the value to prepend.
*   **Logic Flow**:
    1.  (Similar to APPEND) Fetches and decodes main document and value to prepend.
    2.  (Similar to APPEND) Identifies `target_array_ref` using path.
    3.  (Similar to APPEND) Validates path and type.
    4.  Prepends value: `table.insert(target_array_ref, 1, value_to_prepend)`.
    5.  (Similar to APPEND) Encodes and sets the document.
*   **Returns**: The new length of the array after prepending. Redis error on failure.

### 3.6 `JSON_ARRAY_POP_LUA` (Update/Delete)

*   **Purpose**: Removes and returns an element from a JSON array at a specified path and index.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: Path to the array.
    *   `ARGV[2]`: Index of the element to pop (0-based from C++; -1 for last element).
*   **Logic Flow**:
    1.  Fetches and decodes JSON document. If key not found or doc invalid, returns `nil` or error.
    2.  Identifies `target_array_ref` using path. If path invalid or not an array, returns `nil`.
    3.  Converts `index_str` (ARGV[2]) to a number. If not a number, returns `ERR_INDEX`.
    4.  Adjusts index:
        *   Converts C++ 0-based index (or -1 for last) to Lua 1-based index.
        *   E.g., C++ `0` becomes Lua `1`, C++ `-1` becomes Lua `len` (length of array).
        *   If the resulting Lua index is out of bounds (`< 1` or `> len`), returns `nil`.
    5.  Pops element: `local popped_value = table.remove(target_array_ref, lua_index)`.
    6.  Encodes and sets the modified document.
*   **Returns**: JSON string of the popped value, or `nil` if key/path/index invalid or array empty. Redis error on other failures.

### 3.7 `ATOMIC_JSON_GET_SET_PATH_LUA` (Update/Read)

*   **Purpose**: Atomically gets a value at a specified path and then sets a new value at that same path. Returns the old value.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: The JSON path string.
    *   `ARGV[2]`: The JSON string representation of the new value to set.
*   **Logic Flow**:
    1.  Fetches `current_json_str`. If key doesn't exist, initializes `current_doc = {}` and `old_value_encoded = cjson.encode(nil)`.
    2.  Else (key exists):
        *   Decodes `current_json_str`. If fails, returns error.
        *   If path is root (`$`), `old_value_encoded` becomes `cjson.encode(current_doc)`.
        *   Else, parses path, gets `old_value` using `get_value_at_path`, and then `old_value_encoded = cjson.encode(old_value)`.
    3.  Decodes `new_value_json_str` (ARGV[2]) into `new_value`. If fails, returns error.
    4.  If path is root (`$`):
        *   Validates `new_value` type (must be table or null).
        *   `current_doc` is replaced by `new_value`.
    5.  Else (path is not root):
        *   Parses path (again, for setting).
        *   Calls `set_value_at_path(current_doc, path_segments_set, new_value, true)` (create_path is true). If fails, returns error.
    6.  Encodes `current_doc` to `final_doc_str`. If fails, returns error.
    7.  Sets `KEYS[1]` to `final_doc_str`.
*   **Returns**: JSON string of the *old* value that was at the path before setting the new one. Redis error on failure.

### 3.8 `ATOMIC_JSON_COMPARE_SET_PATH_LUA` (Update - Conditional)

*   **Purpose**: Atomically compares a value at a specified path with an expected value. If they match, sets a new value at that path. This is a Compare-And-Swap (CAS) operation.
*   **Arguments**:
    *   `KEYS[1]`: The Redis key.
    *   `ARGV[1]`: The JSON path string.
    *   `ARGV[2]`: JSON string of the *expected* value.
    *   `ARGV[3]`: JSON string of the *new* value to set if comparison succeeds.
*   **Logic Flow**:
    1.  Fetches `current_json_str`.
    2.  Determine `actual_value_at_path`:
        *   If key doesn't exist: If `expected_value_json_str` is `cjson.encode(nil)`, then `actual_value_at_path` is `nil`. Otherwise, comparison will fail, return `0`. Initialize `current_doc = {}`.
        *   If key exists: Decode `current_json_str`. If path is root, `actual_value_at_path` is `current_doc`. Else, parse path and use `get_value_at_path`.
    3.  Encode `actual_value_at_path` to `actual_value_encoded`.
    4.  Comparison: If `actual_value_encoded == expected_value_json_str`:
        *   Decode `new_value_json_str` (ARGV[3]) into `new_value`. If fails, return error.
        *   If path is root (`$`):
            *   Validate `new_value` type.
            *   `current_doc` is replaced by `new_value`.
        *   Else (path is not root):
            *   Parse path.
            *   Call `set_value_at_path(current_doc, path_segments_set, new_value, true)`. If fails, return error.
        *   Encode `current_doc` to `final_doc_str`. If fails, return error.
        *   Set `KEYS[1]` to `final_doc_str`.
        *   Return `1` (success).
    5.  Else (comparison failed):
        *   Return `0` (failure).
*   **Returns**: `1` if comparison succeeded and value was set. `0` if comparison failed. Redis error on other failures.

## 4. Atomicity

Redis guarantees that Lua scripts are executed atomically. This means that once a script begins execution, no other Redis command or another script can run concurrently until the current script completes. This property is fundamental to how RedisJSON++ achieves atomic operations on JSON documents without needing a dedicated module.

For any given JSON manipulation (like setting a field, appending to an array, or a compare-and-set operation), the entire sequence of operations:
1.  Reading the JSON string from a Redis key (`GET key`).
2.  Decoding the JSON string into a Lua data structure.
3.  Modifying this Lua data structure according to the desired logic (e.g., navigating a path, changing a value, adding/removing elements).
4.  Encoding the modified Lua data structure back into a JSON string.
5.  Writing the new JSON string back to the Redis key (`SET key new_value`).

is performed within a single, uninterrupted Lua script execution. From the perspective of any other Redis client, the change to the JSON document appears to happen in a single, indivisible step. There is no intermediate state visible where, for example, a client might read the JSON document after it has been fetched by the script but before it has been modified and written back.

This prevents race conditions that could occur if these steps were performed as separate commands from the client-side, especially in environments with multiple clients accessing the same JSON documents. For instance, without Lua scripts, a client might:
1.  GET the JSON string.
2.  (Another client modifies the same JSON string in Redis).
3.  The first client modifies its local, now stale, version of the JSON.
4.  The first client SETs its modified JSON, potentially overwriting the changes made by the second client.

By using Lua scripts, RedisJSON++ ensures that such interleaved operations on a single JSON document are serialized correctly, maintaining data integrity and consistency for complex JSON modifications. The atomicity also extends to conditional operations like `JSON_PATH_SET_LUA` (with NX/XX) and `ATOMIC_JSON_COMPARE_SET_PATH_LUA`, where the condition check and the subsequent update are performed as one atomic unit.
