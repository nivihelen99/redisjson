#include "redisjson++/json_schema_validator.h"
#include "redisjson++/exceptions.h" // For potential ArgumentInvalidException

namespace redisjson {

// Custom error handler implementation
void JSONSchemaValidator::ErrorHandler::error(
    const nlohmann::json::json_pointer& ptr,
    const json& instance,
    const std::string& message) {
    _errors.push_back("Error at " + ptr.to_string() + ": " + message + ". Instance: " + instance.dump(2));
}

std::vector<std::string> JSONSchemaValidator::ErrorHandler::get_errors() const {
    return _errors;
}

void JSONSchemaValidator::ErrorHandler::clear_errors() {
    _errors.clear();
}


JSONSchemaValidator::JSONSchemaValidator() {}

void JSONSchemaValidator::register_schema(const std::string& schema_name, const json& schema) {
    if (schema_name.empty()) {
        throw ArgumentInvalidException("Schema name cannot be empty.");
    }
    // Basic validation of the schema itself (e.g., is it a valid JSON object?)
    if (!schema.is_object()) {
        throw ArgumentInvalidException("Schema must be a JSON object.");
    }

    std::lock_guard<std::mutex> lock(_mutex);
    try {
        json_validator validator;
        validator.set_root_schema(schema); // This can throw if schema is invalid
        _validators[schema_name] = std::move(validator);
        _raw_schemas[schema_name] = schema;
    } catch (const std::exception& e) {
        throw ArgumentInvalidException("Invalid JSON schema for '" + schema_name + "': " + e.what());
    }
}

bool JSONSchemaValidator::validate(const json& document, const std::string& schema_name) const {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _validators.find(schema_name);
    if (it == _validators.end()) {
        // Or throw SchemaNotFoundException("Schema '" + schema_name + "' not registered.");
        _last_error_handler.clear_errors(); // Clear previous errors
        _last_error_handler.error(nlohmann::json::json_pointer{}, document, "Schema '" + schema_name + "' not registered.");
        return false;
    }

    _last_error_handler.clear_errors(); // Clear previous errors
    // nlohmann::json_schema::json_validator's validate method takes an error_handler by reference.
    // It doesn't directly return bool but populates the error handler.
    // We check if errors were added to determine validity.
    it->second.validate(document, _last_error_handler);

    return _last_error_handler.get_errors().empty();
}

std::vector<std::string> JSONSchemaValidator::get_validation_errors() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _last_error_handler.get_errors();
}

void JSONSchemaValidator::enable_validation(const std::string& key_pattern, const std::string& schema_name) {
    // This is a conceptual placeholder for automatic validation hooks.
    // A full implementation would require:
    // 1. A mechanism to match keys against patterns (e.g., regex or glob).
    // 2. Integration with RedisJSONClient's set/update operations to trigger validation.
    //    - Before a SET operation, the client would check if any pattern matches the key.
    //    - If so, it would call `validate` with the appropriate schema.
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_validators.count(schema_name)) {
        throw ArgumentInvalidException("Schema '" + schema_name + "' not registered. Cannot enable auto-validation.");
    }
    _auto_validation_rules[key_pattern] = schema_name;
    // Note: The client part that USES this rule is not implemented here.
    // This just stores the rule.
    // Consider logging or specific actions if key_pattern is invalid.
}

bool JSONSchemaValidator::is_schema_registered(const std::string& schema_name) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _validators.count(schema_name);
}

} // namespace redisjson
