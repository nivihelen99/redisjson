#pragma once

#include "common_types.h"
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp> // Assuming use of nlohmann's JSON schema validator
#include <unordered_map>
#include <mutex>

namespace redisjson {

using json = nlohmann::json;
using json_validator = nlohmann::json_schema::json_validator;

class JSONSchemaValidator {
public:
    JSONSchemaValidator();

    // Registers a JSON schema with a given name.
    // Throws InvalidArgumentException if schema_name is empty or schema is invalid.
    void register_schema(const std::string& schema_name, const json& schema);

    // Validates a JSON document against a registered schema.
    // Returns true if the document is valid, false otherwise.
    // If schema_name is not found, it will return false.
    bool validate(const json& document, const std::string& schema_name) const;

    // Gets the validation errors from the last validation attempt for a specific schema.
    // This would typically be called after validate() returns false.
    // Note: nlohmann::json-schema validator might handle errors differently,
    // this is a conceptual representation.
    std::vector<std::string> get_validation_errors() const;

    // Enables automatic schema validation for a given key pattern and schema.
    // This is a more advanced feature suggesting integration with RedisJSONClient.
    // For now, this might be a placeholder or require specific design.
    void enable_validation(const std::string& key_pattern, const std::string& schema_name);

    // Checks if a schema is registered.
    bool is_schema_registered(const std::string& schema_name) const;

private:
    class ErrorHandler : public nlohmann::json_schema::basic_error_handler {
    public:
        void error(const nlohmann::json::json_pointer& ptr, const json& instance, const std::string& message) override {
            _errors.push_back("Error at " + ptr.to_string() + ": " + message + " (instance: " + instance.dump() + ")");
        }
        std::vector<std::string> get_errors() const { return _errors; }
        voidclear_errors() { _errors.clear(); }
    private:
        std::vector<std::string> _errors;
    };

    mutable std::unordered_map<std::string, json_validator> _validators;
    mutable std::unordered_map<std::string, json> _raw_schemas;
    mutable ErrorHandler _last_error_handler; // To store errors from the last validation
    mutable std::mutex _mutex;

    // For automatic validation (conceptual)
    std::unordered_map<std::string, std::string> _auto_validation_rules;
};

} // namespace redisjson
