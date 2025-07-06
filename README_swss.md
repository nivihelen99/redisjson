# Using RedisJSONClient with SONiC SWSS

This document describes how to integrate and use the `RedisJSONClient` library with SONiC's Switch State Service (SWSS) `DBConnector` and other `swss-common` components. This mode allows the client to interact with Redis databases managed by SONiC (e.g., APPL_DB, STATE_DB, CONFIG_DB) typically via Unix domain sockets.

## Overview

When configured for SWSS mode, `RedisJSONClient` bypasses its internal Hiredis-based connection pooling and instead utilizes the `swss::DBConnector` provided by the `swss-common` library. This aligns its usage with standard SONiC application development practices.

**Key Changes in SWSS Mode:**

*   **Connection Management:** Handled by `swss::DBConnector`. The library's own `RedisConnectionManager` and `RedisConnection` classes are not used.
*   **Configuration:** A new configuration structure, `SwssClientConfig`, is used.
*   **Path/Array Operations:** Operations that modify parts of a JSON document (e.g., `set_path`, `append_path`, `pop_path`) are performed client-side. This involves a get-modify-set pattern:
    1.  The entire JSON document is read from Redis.
    2.  The modification is applied to the JSON object in memory (using `nlohmann::json` capabilities via the internal `JSONModifier` component).
    3.  The entire modified JSON document is written back to Redis.
    *   **Important:** This means these operations are **not atomic** in SWSS mode, unlike the Lua script-based atomicity provided in the legacy direct-Redis connection mode.
*   **Atomic Operations:** Methods like `atomic_get_set` and `atomic_compare_set` are replaced by `non_atomic_get_set` and `non_atomic_compare_set`. These also follow a non-atomic get-compare-modify-set client-side pattern. True atomic operations on sub-document paths are not supported in SWSS mode through this client as `DBConnector` does not typically support arbitrary Lua scripting (`EVAL`/`EVALSHA`) required for this.
*   **Lua Scripts:** The `LuaScriptManager` is not used in SWSS mode. All operations are either direct Redis commands compatible with `DBConnector`'s interface or client-side manipulations of fetched JSON data.

## Configuration

To use `RedisJSONClient` in SWSS mode, you must initialize it with `SwssClientConfig`.

### `SwssClientConfig` Structure

Defined in `include/redisjson++/common_types.h`:

```cpp
struct SwssClientConfig {
    // Name of the database to connect to (e.g., "APPL_DB", "STATE_DB", "CONFIG_DB")
    // Or an integer DB ID if your DBConnector instance primarily uses integer IDs.
    std::string db_name;

    // Timeout for database operations (in milliseconds)
    unsigned int operation_timeout_ms = 5000;

    // Path to the Redis Unix domain socket.
    // This is typically fixed in a SONiC environment.
    std::string unix_socket_path = "/var/run/redis/redis.sock";

    // Whether DBConnector should wait for the database to be ready upon connection.
    bool wait_for_db = false;
};
```

### Example Initialization

```cpp
#include "redisjson++/redis_json_client.h"
#include "redisjson++/common_types.h"
#include "redisjson++/exceptions.h" // For error handling

// ...

redisjson::SwssClientConfig config;
config.db_name = "APPL_DB"; // Target the Application Database
// config.unix_socket_path = "/var/run/redis/redis.sock"; // Default is usually fine
// config.operation_timeout_ms = 5000; // Default
// config.wait_for_db = false; // Default

try {
    redisjson::RedisJSONClient client(config);
    std::cout << "Successfully connected to " << config.db_name << std::endl;
    // Use the client...
} catch (const redisjson::ConnectionException& e) {
    std::cerr << "Connection failed: " << e.what() << std::endl;
} catch (const redisjson::RedisJSONException& e) {
    std::cerr << "RedisJSONClient error: " << e.what() << std::endl;
}
```

## Using the Client (SWSS Mode)

### Document Operations

These operations work on top-level Redis keys, where the value is a JSON string.

*   **`set_json(const std::string& key, const json& document, const SetOptions& opts = {})`**
    *   Serializes the `nlohmann::json` document to a string and stores it in Redis under `key`.
    *   **Note on `SetOptions` in SWSS Mode:**
        *   `ttl`: If `DBConnector`'s `set` method (or equivalent raw command execution path) supports `EX` (expire), TTL will be honored. If the underlying `DBConnector` interface is a basic `SET key value`, TTL might be ignored. The current implementation attempts to use extended SET commands if the (internal, assumed) `DBConnector` API allows, but this depends heavily on the actual `swss::DBConnector`.
        *   `condition` (NX/XX): Similar to TTL, support depends on the `DBConnector`'s capability to pass these conditions to the Redis SET command.
        *   `create_path`: Not applicable to `set_json`.

    ```cpp
    json user_profile = {
        {"name", "John Doe"},
        {"email", "john.doe@example.com"}
    };
    redisjson::SetOptions set_opts;
    // set_opts.ttl = std::chrono::seconds(3600); // Example TTL
    client.set_json("user:123", user_profile, set_opts);
    ```

*   **`get_json(const std::string& key) const`**
    *   Retrieves the JSON string from Redis for `key` and parses it into `nlohmann::json`.
    *   Throws `PathNotFoundException` if the key does not exist.
    *   Throws `JsonParsingException` if the stored value is not valid JSON.

    ```cpp
    try {
        json profile = client.get_json("user:123");
        std::cout << "User name: " << profile.value("name", "N/A") << std::endl;
    } catch (const redisjson::PathNotFoundException& e) {
        std::cerr << "User profile not found: " << e.what() << std::endl;
    }
    ```

*   **`del_json(const std::string& key)`**
    *   Deletes the key from Redis.

*   **`exists_json(const std::string& key) const`**
    *   Checks if the key exists in Redis.

### Path Operations (Non-Atomic in SWSS Mode)

These operations target specific parts within a JSON document. In SWSS mode, they are **not atomic** due to the client-side get-modify-set implementation.

*   **`get_path(const std::string& key, const std::string& path_str) const`**
    *   Retrieves the JSON document for `key`, then extracts the value at `path_str`.
    *   `path_str` uses a dot-notation for objects and `[index]` for arrays (e.g., `"address.city"`, `"hobbies[0]"`).
    *   Throws `PathNotFoundException` if the key or the path within the JSON does not exist.

    ```cpp
    try {
        json city = client.get_path("user:123", "address.city");
        std::cout << "City: " << city.dump() << std::endl;
    } catch (const redisjson::PathNotFoundException& e) {
        std::cerr << "Path not found: " << e.what() << std::endl;
    }
    ```

*   **`set_path(const std::string& key, const std::string& path_str, const json& value, const SetOptions& opts = {})`**
    *   **Non-Atomic:** Fetches the document, modifies it in memory, and writes it back.
    *   `opts.create_path` (defaults to `true` in `SetOptions` struct, and `JSONModifier::set` also defaults `create_path` to true): If `true`, intermediate paths (objects or arrays) will be created if they don't exist.
    *   `opts.ttl` and `opts.condition` apply to the final `SET` of the entire document.

    ```cpp
    client.set_path("user:123", "address.zip", "90210");
    // To add a new hobby, assuming "hobbies" is an array:
    // client.set_path("user:123", "hobbies[1]", "coding"); // This would overwrite or create index 1
    ```

*   **`del_path(const std::string& key, const std::string& path_str)`**
    *   **Non-Atomic:** Fetches, modifies (removes element at path), writes back.
    *   If the path does not exist, the operation is a no-op.

*   **`exists_path(const std::string& key, const std::string& path_str) const`**
    *   Fetches the document and checks if the path exists within it.

### Array Operations (Non-Atomic in SWSS Mode)

These also operate client-side in SWSS mode and are **not atomic**.

*   **`append_path(const std::string& key, const std::string& path_str, const json& value)`**
    *   Appends `value` to the JSON array at `path_str`.
    *   **Non-Atomic.**

*   **`prepend_path(const std::string& key, const std::string& path_str, const json& value)`**
    *   Prepends `value` to the JSON array at `path_str`.
    *   **Non-Atomic.**

*   **`pop_path(const std::string& key, const std::string& path_str, int index = -1)`**
    *   Removes and returns an element from the array at `path_str`. `index` defaults to -1 (last element).
    *   **Non-Atomic.**

*   **`array_length(const std::string& key, const std::string& path_str) const`**
    *   Returns the length of the JSON array at `path_str`.

### Merge and Patch Operations (Client-Side in SWSS Mode)

*   **`merge_json(const std::string& key, const json& patch)`**
    *   **Non-Atomic (Client-Side):** Fetches the document for `key`, performs a client-side merge using `nlohmann::json::merge_patch` (RFC 7386) semantics with the `patch` document, and writes the result back.
    *   This differs from the legacy mode's direct use of RedisJSON `JSON.MERGE` command, which has specific deep merge semantics. `merge_patch` has different rules (e.g., null values in patch cause deletions).

*   **`patch_json(const std::string& key, const json& patch_operations)`**
    *   **Non-Atomic (Client-Side):** Applies JSON Patch (RFC 6902) operations. Fetches the document, applies `patch_operations` using `nlohmann::json::patch`, and writes back.

### "Atomic" Operations (Non-Atomic in SWSS Mode)

The original atomic operations are renamed and their behavior changes in SWSS mode.

*   **`non_atomic_get_set(const std::string& key, const std::string& path_str, const json& new_value)`**
    *   **Non-Atomic:** Fetches the document, gets the value at `path_str`, updates the value at `path_str` to `new_value` (creating the path if necessary), writes the document back, and returns the *original* value at the path.

*   **`non_atomic_compare_set(const std::string& key, const std::string& path_str, const json& expected_val, const json& new_val)`**
    *   **Non-Atomic:** Fetches the document. If the value at `path_str` matches `expected_val` (or if the path doesn't exist and `expected_val` is `null`), it updates the value at `path_str` to `new_val` (creating path if needed) and writes the document back. Returns `true` if the set was performed, `false` otherwise.

### Utility Operations

*   **`keys_by_pattern(const std::string& pattern) const`**
    *   In SWSS mode, this attempts to use `_db_connector->keys(pattern)`. The behavior (e.g., whether it uses `KEYS` or `SCAN`) depends on the `swss::DBConnector` implementation. Be cautious with patterns like `*` on production systems if it maps to a blocking `KEYS` command.

*   **`search_by_value(const std::string& key, const json& search_value) const`**
    *   Client-side search. Fetches the entire document for `key` and recursively searches for `search_value`.

*   **`get_all_paths(const std::string& key) const`**
    *   Client-side. Fetches the document for `key` and generates a list of all possible JSON paths within it.

## Porting from Legacy (Direct Redis) Mode

1.  **Configuration:**
    *   Change `LegacyClientConfig` to `SwssClientConfig`.
    *   Update parameters from host/port/password to `db_name`, `unix_socket_path`.

    **Before:**
    ```cpp
    redisjson::LegacyClientConfig config;
    config.host = "127.0.0.1";
    config.port = 6379;
    redisjson::RedisJSONClient client(config);
    ```

    **After (SWSS):**
    ```cpp
    redisjson::SwssClientConfig swss_config;
    swss_config.db_name = "APPL_DB";
    // swss_config.unix_socket_path = "/var/run/redis/redis.sock"; // Often default
    redisjson::RedisJSONClient client(swss_config);
    ```

2.  **Atomic Operations:**
    *   Replace calls to `atomic_get_set` with `non_atomic_get_set`.
    *   Replace calls to `atomic_compare_set` with `non_atomic_compare_set`.
    *   **Crucially, understand that these operations are no longer atomic.** If atomicity is critical, application-level locking mechanisms or a different approach to data modification within SONiC might be required.

3.  **Path/Array Operations:**
    *   The method signatures remain the same.
    *   Be aware that these are now non-atomic and involve full document reads/writes. This can have performance implications for very large JSON documents or frequent partial updates.

4.  **Error Handling:**
    *   `ConnectionException` can still be thrown if `DBConnector` fails to connect.
    *   `RedisCommandException` may be thrown by `DBConnector` operations or by client-side logic if internal errors occur.
    *   `PathNotFoundException`, `JsonParsingException`, etc., remain relevant.

5.  **Lua Scripts:**
    *   If your application relied on custom Lua scripts executed via `LuaScriptManager`, this functionality is not available in SWSS mode. The logic of those scripts must be reimplemented in C++ on the client-side (likely involving get-modify-set) or re-evaluated for the SONiC environment.

## Assumptions and Limitations in SWSS Mode

*   **`swss::DBConnector` API:** This port assumes a certain common API for `DBConnector` (e.g., methods like `set(key, value)`, `get(key)`, `del(key)`, `exists(key)`, `keys(pattern)`). The exact behavior of these methods (e.g., how errors are reported, how `nil` replies are handled for `get`) might vary slightly with the actual `swss-common` version and implementation.
*   **No Direct JSON Type in Redis:** The client assumes JSON documents are stored as strings in Redis. `DBConnector` is expected to handle string values.
*   **Performance of Client-Side Operations:** Get-modify-set for path/array operations can be less performant than server-side manipulations (like RedisJSON module commands or Lua scripts), especially for large documents or high-frequency updates.
*   **Atomicity:** As stressed before, atomicity for sub-document modifications is lost.
*   **`JSONModifier` and `PathParser` Implementation:** The correctness of client-side path and array operations heavily depends on the internal `JSONModifier` and `PathParser` components being robust and correctly interpreting/manipulating `nlohmann::json` objects according to the path expressions.

## Building and Linking

When building an application that uses `RedisJSONClient` in SWSS mode, ensure you link against:
*   This `redisjson` library.
*   `swss-common` (which provides `DBConnector`).
*   `hiredis` (if `swss-common` doesn't statically link it or if your project needs it directly for other reasons, though this client aims to abstract direct Hiredis in SWSS mode).
*   `nlohmann-json`.

Consult the SONiC development environment documentation for specific instructions on include paths and linking for SWSS applications.
The `CMakeLists.txt` for this library would need to be adapted to correctly find and link `swss-common` when building for a SONiC target.
```
