#pragma once

#include "common_types.h"
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

namespace redisjson {

using json = nlohmann::json;

class JSONQueryEngine {
public:
    // Forward declaration of RedisJSONClient to avoid circular dependency
    class RedisJSONClient;

    explicit JSONQueryEngine(RedisJSONClient& client);

    // JSONPath Support
    // Executes a JSONPath query against the JSON document stored at the given key.
    // Returns a vector of JSON values that match the query.
    std::vector<json> query(const std::string& key,
                           const std::string& jsonpath) const;

    // SQL-like Queries (Placeholder - Implementation requires significant effort)
    // Executes an SQL-like query against the JSON document.
    // This is a complex feature and might require a dedicated parsing and execution engine.
    std::vector<json> select(const std::string& key,
                            const std::string& where_clause) const;

    // Aggregation (Placeholder - Implementation requires significant effort)
    // Performs aggregation operations (sum, avg, count, etc.) on values found at a specific path.
    json aggregate(const std::string& key, const std::string& path,
                  const std::string& operation) const;

private:
    RedisJSONClient& _client;
    // Helper methods for query execution might be needed here
};

} // namespace redisjson
