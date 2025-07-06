#pragma once

#include <string>
#include <vector>
#include <chrono> // For std::chrono::seconds
#include <cstdint> // For std::uint16_t

namespace redisjson {

// Enum for SET command conditions (NX, XX)
enum class SetCmdCondition {
    NONE, // No condition
    NX,   // Set only if key does not exist
    XX    // Set only if key already exists
};

// Configuration for the Redis client when using direct Redis connection
struct LegacyClientConfig {
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string password; // Empty if no password
    int database = 0;     // Default Redis database
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000); // Connection and command timeout

    // Connection pool settings
    int connection_pool_size = 5; // Max total connections in the pool
    int min_idle_connections = 1; // Minimum number of idle connections to keep
    std::chrono::seconds health_check_interval = std::chrono::seconds(30);
    std::chrono::seconds connection_max_idle_time = std::chrono::seconds(300); // Max time a connection can be idle before being potentially closed
    bool enable_tcp_keepalives = true;
    std::uint16_t tcp_keepalive_time_sec = 60; // Time in seconds for TCP keepalive

    // Retry strategy (basic example)
    int max_retries = 3;
    std::chrono::milliseconds retry_backoff_start = std::chrono::milliseconds(100);
};

// Configuration for the RedisJSONClient when used with SONiC SWSS
struct SwssClientConfig {
    // Name of the database to connect to (e.g., "APPL_DB", "STATE_DB", "CONFIG_DB")
    // Or an integer DB ID if DBConnector supports that primarily.
    std::string db_name;
    // Timeout for database operations
    unsigned int operation_timeout_ms = 5000;
    // Path to the Redis Unix domain socket, typically fixed in SONiC.
    // If empty, DBConnector might use a default or TCP. For SONiC, this should be set.
    std::string unix_socket_path = "/var/run/redis/redis.sock";
    // Whether DBConnector should wait for the database to be ready on connect.
    bool wait_for_db = false;

    // If all JSON documents are to be stored within a single Redis HASH (table),
    // specify the table name here. If empty, JSON documents are stored as top-level keys.
    // std::string default_table_name;
};


// Options for SET operations in RedisJSONClient
struct SetOptions {
    bool create_path = true;        // For path-specific operations (not used by current set_json with SWSS)
                                    // When using SWSS, path operations are client-side get-modify-set.
                                    // This option is less relevant for the SET command itself.
    // bool overwrite = true;       // For Redis SET, this is implicit. SWSS set will overwrite.
    std::chrono::seconds ttl = std::chrono::seconds(0); // TTL (0 = no expiry). SETEX will be used if > 0.
    SetCmdCondition condition = SetCmdCondition::NONE; // Conditional set (NX, XX). SET key value NX/XX
};

// Keep ClientConfig as an alias for now for easier transition in examples,
// but new code should prefer SwssClientConfig or LegacyClientConfig explicitly.
using ClientConfig = LegacyClientConfig;

} // namespace redisjson
