#pragma once

#include <string>
#include <vector>
#include <chrono> // For std::chrono::seconds
#include <cstdint> // For std::uint16_t

namespace redisjson {

// Configuration for the Redis client and connection manager
struct ClientConfig {
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string password; // Empty if no password
    int database = 0;     // Default Redis database
    std::chrono::milliseconds timeout = std::chrono::milliseconds(500); // Connection and command timeout

    // Connection pool settings
    int connection_pool_size = 5; // Max total connections in the pool
    int min_idle_connections = 1; // Minimum number of idle connections to keep
    std::chrono::seconds health_check_interval = std::chrono::seconds(30);
    std::chrono::seconds connection_max_idle_time = std::chrono::seconds(300); // Max time a connection can be idle before being potentially closed
    bool enable_tcp_keepalives = true;
    std::uint16_t tcp_keepalive_time_sec = 60; // Time in seconds for TCP keepalive

// Enum for SET command conditions (NX, XX)
enum class SetCmdCondition {
    NONE, // No condition
    NX,   // Set only if key does not exist
    XX    // Set only if key already exists
};

    // Retry strategy (basic example)
    int max_retries = 3;
    std::chrono::milliseconds retry_backoff_start = std::chrono::milliseconds(100);

    // Add other configurations as needed:
    // bool use_tls = false;
    // std::string tls_ca_cert_path;
    // std::string tls_client_cert_path;
    // std::string tls_client_key_path;
    // std::string sentinel_master_name;
    // std::vector<std::pair<std::string, int>> sentinel_nodes;
};

// Options for SET operations in RedisJSONClient
struct SetOptions {
    bool create_path = true;        // For path-specific operations (not used by current set_json)
    bool overwrite = true;          // For Redis SET, this is implicit. For JSON specific ops, might differ.
    std::chrono::seconds ttl = std::chrono::seconds(0); // TTL (0 = no expiry)
    SetCmdCondition condition = SetCmdCondition::NONE; // Conditional set (NX, XX)
    // bool compress = false;          // Future feature
    // bool validate_schema = false;   // Future feature
    // std::string schema_name = "";   // Future feature
    // bool emit_events = true;        // Future feature
};

} // namespace redisjson
