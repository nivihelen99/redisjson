#pragma once

#include <stdexcept>
#include <string>
#include <optional> // For optional error_code in base exception

namespace redisjson {

// Error codes as defined in requirements
enum class ErrorCode {
    SUCCESS = 0,
    INVALID_PATH = 1001,
    PATH_NOT_FOUND = 1002,
    TYPE_MISMATCH = 1003,
    CONNECTION_FAILED = 2001,
    TIMEOUT = 2002, // Specific connection timeout error
    LUA_SCRIPT_ERROR = 3001,
    VALIDATION_FAILED = 4001,
    TRANSACTION_FAILED = 5001,
    JSON_PARSING_ERROR = 6001, // Added for JsonParsingException
    INDEX_OUT_OF_BOUNDS = 6002, // Added for IndexOutOfBoundsException
    OPERATION_ABORTED = 7001, // Added for OperationAbortedException
    REDIS_COMMAND_ERROR = 8001, // General Redis command error not covered by others
    UNKNOWN_ERROR = 9999
};

// Base exception for the library
class RedisJSONException : public std::runtime_error {
public:
    explicit RedisJSONException(const std::string& message, std::optional<ErrorCode> code = std::nullopt)
        : std::runtime_error(message), error_code_(code) {}

    std::optional<ErrorCode> error_code() const { return error_code_; }

    // Consider adding a virtual method for more detailed error messages if needed
    // virtual std::string error_details() const { return ""; }

private:
    std::optional<ErrorCode> error_code_;
};

// -- Path Related Exceptions --
class InvalidPathException : public RedisJSONException {
public:
    explicit InvalidPathException(const std::string& message)
        : RedisJSONException("Invalid Path: " + message, ErrorCode::INVALID_PATH) {}
};

class PathNotFoundException : public RedisJSONException {
public:
    explicit PathNotFoundException(const std::string& path_str)
        : RedisJSONException("Path not found: " + path_str, ErrorCode::PATH_NOT_FOUND) {}
     PathNotFoundException(const std::string& key, const std::string& path_str)
        : RedisJSONException("Path not found for key '" + key + "': " + path_str, ErrorCode::PATH_NOT_FOUND) {}
};


// -- JSON Processing Exceptions --
class JsonParsingException : public RedisJSONException {
public:
    explicit JsonParsingException(const std::string& message)
        : RedisJSONException("JSON Parsing Error: " + message, ErrorCode::JSON_PARSING_ERROR) {}
};

class TypeMismatchException : public RedisJSONException {
public:
    explicit TypeMismatchException(const std::string& message)
        : RedisJSONException("JSON Type Mismatch: " + message, ErrorCode::TYPE_MISMATCH) {}
     TypeMismatchException(const std::string& path_str, const std::string& expected_type, const std::string& actual_type)
        : RedisJSONException("JSON Type Mismatch at path '" + path_str + "'. Expected " + expected_type + ", got " + actual_type, ErrorCode::TYPE_MISMATCH) {}
};


// -- Redis Communication Exceptions --
class ConnectionException : public RedisJSONException {
public:
    explicit ConnectionException(const std::string& message, ErrorCode code = ErrorCode::CONNECTION_FAILED)
        : RedisJSONException("Redis Connection Error: " + message, code) {}
};

// Specific timeout exception
class TimeoutException : public ConnectionException {
public:
    explicit TimeoutException(const std::string& message)
        : ConnectionException(message, ErrorCode::TIMEOUT) {}
};


class RedisCommandException : public RedisJSONException {
public:
    explicit RedisCommandException(const std::string& message)
        : RedisJSONException("Redis Command Error: " + message, ErrorCode::REDIS_COMMAND_ERROR) {}
    RedisCommandException(const std::string& command, const std::string& details)
        : RedisJSONException("Redis Command Error for '" + command + "': " + details, ErrorCode::REDIS_COMMAND_ERROR) {}
};

class LuaScriptException : public RedisJSONException {
public:
    explicit LuaScriptException(const std::string& script_name, const std::string& message)
        : RedisJSONException("Lua Script Error in '" + script_name + "': " + message, ErrorCode::LUA_SCRIPT_ERROR) {}
};


// -- Transaction and Operation Exceptions --
class TransactionException : public RedisJSONException {
public:
    explicit TransactionException(const std::string& message)
        : RedisJSONException("Transaction Error: " + message, ErrorCode::TRANSACTION_FAILED) {}
};

class OperationAbortedException : public RedisJSONException {
public:
    explicit OperationAbortedException(const std::string& message)
        : RedisJSONException("Operation Aborted: " + message, ErrorCode::OPERATION_ABORTED) {}
};


// -- Schema Validation (if implemented) --
class ValidationException : public RedisJSONException {
public:
    explicit ValidationException(const std::string& message)
        : RedisJSONException("Schema Validation Error: " + message, ErrorCode::VALIDATION_FAILED) {}
};


// -- Other Specific Errors --
class IndexOutOfBoundsException : public RedisJSONException {
public:
    explicit IndexOutOfBoundsException(const std::string& message)
        : RedisJSONException("Index Out of Bounds: " + message, ErrorCode::INDEX_OUT_OF_BOUNDS) {}
    IndexOutOfBoundsException(int index, size_t array_size)
        : RedisJSONException("Index Out of Bounds: index " + std::to_string(index) + " on array of size " + std::to_string(array_size), ErrorCode::INDEX_OUT_OF_BOUNDS) {}
};


} // namespace redisjson
