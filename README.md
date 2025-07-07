
# RedisJSON++

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/YOUR_ORG/YOUR_REPO/actions) <!-- Placeholder: Update with your CI build status badge -->
[![Coverage](https://img.shields.io/badge/coverage-XX%25-good)](https://coveralls.io/YOUR_ORG/YOUR_REPO) <!-- Placeholder: Update with your code coverage badge -->
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md) <!-- Assuming an MIT License file named LICENSE.md -->
[![Version](https://img.shields.io/badge/version-1.0.0-blue)](https://github.com/YOUR_ORG/YOUR_REPO/releases/tag/v1.0.0) <!-- Placeholder: Update with your release version -->
[![C++ Standard](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey)](https://isocpp.org/)

RedisJSON++ is a high-performance C++17 library that provides native JSON manipulation capabilities over Redis without requiring the RedisJSON module. It enables applications to store, query, and manipulate JSON documents as Redis strings while providing an intuitive, type-safe API that abstracts away Redis's string-based operations.

## Key Features

- **Zero Dependencies on Redis Modules**: Works with any standard Redis instance (version >= 5.0).
- **High Performance**: Optimized for minimal network roundtrips and memory usage, leveraging Lua scripting for atomicity.
- **Type Safety**: Modern C++ design with `nlohmann/json` for JSON handling.
- **Intuitive API**: Designed to feel natural for C++ developers.
- **Atomic Operations**: Supports atomic JSON modifications using embedded Lua scripts.
- **Comprehensive JSON Operations**: Full support for document-level, path-level, and array operations.
- **Connection Management**: Built-in connection pooling and management.
- **Extensive Error Handling**: Clear exception hierarchy for robust application development.
- **Cross-Platform**: Compatible with Linux, macOS, and Windows.

## Table of Contents

- [Installation](#installation)
  - [CMake with FetchContent](#cmake-with-fetchcontent)
  - [vcpkg (Planned)](#vcpkg-planned)
  - [Conan (Planned)](#conan-planned)
- [Quick Start](#quick-start)
- [Detailed Usage Examples](#detailed-usage-examples)
  - [Document Operations](#document-operations)
  - [Path Operations](#path-operations)
  - [Array Operations](#array-operations)
  - [Atomic Operations](#atomic-operations)
- [API Overview](#api-overview)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Building from Source](#building-from-source)
  - [Prerequisites](#prerequisites)
  - [Build Steps](#build-steps)
- [Running Tests](#running-tests)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)
- [Project Structure](#project-structure)

## Installation

**Note:** Replace `https://github.com/YOUR_ORG/YOUR_REPO.git` with the actual repository URL.

### CMake with `FetchContent`

Add the following to your `CMakeLists.txt`:

```cmake
include(FetchContent)

FetchContent_Declare(
  redisjson_plus_plus
  GIT_REPOSITORY https://github.com/YOUR_ORG/YOUR_REPO.git # Replace with actual URL
  GIT_TAG v1.0.0 # Or desired version/commit
)
FetchContent_MakeAvailable(redisjson_plus_plus)

# Link to your target (the library is named redisjson++ as per its CMakeLists.txt)
target_link_libraries(your_target_name PRIVATE redisjson++)
```

### vcpkg (Planned)
Support for vcpkg is planned.
```bash
# Example: vcpkg install redisjson-plus-plus 
```

### Conan (Planned)
Support for Conan is planned.
```bash
# Example: conan install redisjson-plus-plus/1.0.0@
```

## Quick Start

Here's a simple example to get you started:

```cpp
#include "redisjson++/redis_json_client.h"
#include <nlohmann/json.hpp>
#include <iostream>

// Alias for convenience
using json = nlohmann::json;

int main() {
    redisjson::ClientConfig config;
    // Default config connects to "127.0.0.1" on port 6379.
    // Modify if your Redis instance is elsewhere or requires a password:
    // config.host = "your_redis_host";
    // config.port = your_redis_port;
    // config.password = "your_redis_password";

    try {
        redisjson::RedisJSONClient client(config);
        std::cout << "Successfully connected to Redis!" << std::endl;

        std::string user_key = "user:1001";
        json user_data = {
            {"name", "Alice Wonderland"},
            {"email", "alice@example.com"},
            {"age", 30}
        };

        // Set a JSON document
        client.set_json(user_key, user_data);
        std::cout << "Set document for key: " << user_key << std::endl;

        // Get the document
        json retrieved_data = client.get_json(user_key);
        std::cout << "Retrieved document: " << retrieved_data.dump(2) << std::endl;

        // Get a specific field (path uses dot notation)
        json email = client.get_path(user_key, "email");
        std::cout << "User's email: " << email.get<std::string>() << std::endl;

        // Modify a field
        client.set_path(user_key, "age", 31);
        json new_age = client.get_path(user_key, "age");
        std::cout << "User's age updated to: " << new_age.get<int>() << std::endl;

        // Delete the document
        client.del_json(user_key);
        std::cout << "Deleted document for key: " << user_key << std::endl;

    } catch (const redisjson::ConnectionException& e) {
        std::cerr << "Connection Error: " << e.what() << std::endl;
        std::cerr << "Please ensure Redis is running and accessible at "
                  << config.host << ":" << config.port << std::endl;
        return 1;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "RedisJSON++ Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
```

## Detailed Usage Examples

For more comprehensive examples, please refer to the `examples/sample.cpp` file in the repository. Below are snippets demonstrating common operations.

*(Assume `redisjson::RedisJSONClient client;` is initialized as shown in Quick Start)*

### Document Operations

```cpp
std::string doc_key = "user:profile:1";
json user_profile = {
    {"name", "John Doe"},
    {"email", "john.doe@example.com"},
    {"age", 30}
};

// Set a JSON document
// redisjson::SetOptions opts; // Optional: configure TTL, etc.
// client.set_json(doc_key, user_profile, opts);
client.set_json(doc_key, user_profile);

// Get the full JSON document
json retrieved_doc = client.get_json(doc_key);
std::cout << "Retrieved: " << retrieved_doc.dump(2) << std::endl;

// Check if a document exists
if (client.exists_json(doc_key)) {
    std::cout << "Document " << doc_key << " exists." << std::endl;
}

// Delete a document
client.del_json(doc_key);
```

### Path Operations

Path operations allow you to interact with specific parts of a JSON document. Paths are specified using a dot-notation (e.g., `contact.email`) or array indexing (e.g., `hobbies[0]`). The root of the document is typically represented by `$` (though often implicit).

```cpp
// Assuming 'doc_key' contains the user_profile from above
client.set_json(doc_key, user_profile); // Ensure it exists

// Get a value at a specific path
json email = client.get_path(doc_key, "email");
std::cout << "Email: " << email.get<std::string>() << std::endl;

// Set a value at a specific path (overwrites if exists, creates if not and create_path is true in options)
client.set_path(doc_key, "age", 31);
client.set_path(doc_key, "address.city", "New York"); // Creates address object and city field

// Check if a path exists
if (client.exists_path(doc_key, "address.city")) {
    std::cout << "Path 'address.city' exists." << std::endl;
}

// Delete a value at a path
client.del_path(doc_key, "age");
```

### Array Operations

```cpp
std::string list_key = "user:hobbies:1";
json hobbies_data = { {"hobbies", {"reading", "cycling"}} };
client.set_json(list_key, hobbies_data);

// Append to an array
client.append_path(list_key, "hobbies", "photography");

// Prepend to an array
client.prepend_path(list_key, "hobbies", "hiking");

// Get array length
size_t length = client.array_length(list_key, "hobbies");
std::cout << "Number of hobbies: " << length << std::endl; // Should be 4 after appends/prepends

// Pop an element from an array (default: last element, index -1)
json last_hobby = client.pop_path(list_key, "hobbies"); // or client.pop_path(list_key, "hobbies", -1);
std::cout << "Popped last hobby: " << last_hobby.dump() << std::endl;

// Pop an element from a specific index (e.g., first element)
json first_hobby = client.pop_path(list_key, "hobbies", 0);
std::cout << "Popped first hobby: " << first_hobby.dump() << std::endl;
```

### Atomic Operations

RedisJSON++ supports atomic operations, typically implemented via Lua scripts on the Redis server, to prevent race conditions when modifying JSON data.

```cpp
std::string counter_key = "system:counter:page_views";
// Initialize if it doesn't exist or set to a known state
client.set_json(counter_key, {{"count", 0}, {"version", 1}});

// Atomically get the current value at path "count" and set it to 10
// The actual atomicity depends on the successful execution of the underlying Lua script.
json old_count = client.atomic_get_set(counter_key, "count", 10);
std::cout << "Old count: " << old_count.dump() << ", New count: "
          << client.get_path(counter_key, "count").dump() << std::endl;

// Atomically compare the value at path "version" with 1. If it matches, set it to 2.
bool cas_success = client.atomic_compare_set(counter_key, "version", 1, 2);
if (cas_success) {
    std::cout << "CAS successful. Version is now 2." << std::endl;
} else {
    std::cout << "CAS failed. Version was not 1 (or path didn't exist)." << std::endl;
}
```

## API Overview

The primary interface for interacting with RedisJSON++ is the `redisjson::RedisJSONClient` class.

Key classes and types:
- **`RedisJSONClient`**: The main entry point for all operations. Manages connections and orchestrates commands.
- **`ClientConfig`**: Struct to configure client parameters like Redis host, port, password, connection pool size, timeouts, etc. (see `redisjson++/common_types.h`).
- **`SetOptions`**: Struct to specify options for `set` operations (e.g., TTL, whether to create paths if they don't exist). (see `redisjson++/common_types.h`).
- **`RedisJSONException`** (and its derivatives): Base class for exceptions thrown by the library (see `redisjson++/exceptions.h`).

Refer to the header files in `include/redisjson++/` for detailed API contracts, especially:
- `redis_json_client.h`
- `common_types.h`
- `exceptions.h`

The `docs/requirement.md` file also provides an extensive overview of the intended API and features.

## Configuration

The `redisjson::RedisJSONClient` is configured using the `redisjson::ClientConfig` struct passed to its constructor.

```cpp
#include "redisjson++/common_types.h" // For ClientConfig
#include "redisjson++/redis_json_client.h"
#include <chrono> // For std::chrono::milliseconds

// ...

redisjson::ClientConfig config;
config.host = "127.0.0.1";
config.port = 6379;
// config.password = "your_secret_password"; // If Redis requires authentication
config.database = 0; // Redis database number
config.connection_pool_size = 10; // Max number of connections in the pool
config.timeout = std::chrono::milliseconds(5000); // Connection and command timeout

// See docs/requirement.md or common_types.h for more options like:
// config.enable_compression = false;
// config.max_retries = 3;
// config.retry_delay = std::chrono::milliseconds(100);

redisjson::RedisJSONClient client(config);
```

## Error Handling

RedisJSON++ uses a hierarchy of exceptions derived from `std::exception` via `redisjson::RedisJSONException`. This allows for granular error handling.

```cpp
#include "redisjson++/exceptions.h" // For specific exception types
#include "redisjson++/redis_json_client.h" // For RedisJSONClient
#include <iostream>

// ... redisjson::RedisJSONClient client; ...

try {
    // Example: Attempt to get a value from a non-existent path
    json value = client.get_path("mykey", "non.existent.path");
} catch (const redisjson::PathNotFoundException& e) {
    std::cerr << "Error: Path not found. Key: " << e.key() 
              << ", Path: " << e.path() << ". Details: " << e.what() << std::endl;
} catch (const redisjson::ConnectionException& e) {
    std::cerr << "Error: Could not connect to Redis. Details: " << e.what() << std::endl;
} catch (const redisjson::TypeMismatchException& e) {
    std::cerr << "Error: Type mismatch in JSON operation. Details: " << e.what() << std::endl;
} catch (const redisjson::RedisJSONException& e) { // Catch-all for other library-specific errors
    std::cerr << "A RedisJSON++ error occurred: " << e.what() << std::endl;
} catch (const std::exception& e) { // Catch other standard C++ errors (e.g., from nlohmann/json)
    std::cerr << "A standard C++ error occurred: " << e.what() << std::endl;
}
```
Key exception types (see `redisjson++/exceptions.h` and `docs/requirement.md` for a more complete list):
- `ConnectionException`: For issues related to connecting to Redis.
- `PathNotFoundException`: If a specified JSON path does not exist in the document.
- `InvalidPathException`: If the JSON path syntax is invalid.
- `TypeMismatchException`: If an operation is attempted on an incompatible JSON type.
- `LuaScriptException`: For errors related to Lua script execution.
- `ValidationException`: For schema validation failures.
- `TransactionException`: For errors during transactions.

## Building from Source

### Prerequisites

- **C++17 Compiler**: A modern C++ compiler supporting C++17 (e.g., GCC 7+, Clang 6+, MSVC VS 2017+).
- **CMake**: Version 3.16 or higher.
- **hiredis**: The Redis C client library. It must be installed on your system and discoverable by `pkg-config`.
  - On Debian/Ubuntu: `sudo apt-get install libhiredis-dev`
  - On Fedora/CentOS: `sudo dnf install hiredis-devel`
  - On macOS (using Homebrew): `brew install hiredis`
- **nlohmann/json**: This library is included as a third-party dependency within the repository (`thirdparty/nlohmann/json.hpp`) and does not require separate installation.
- **GoogleTest**: Fetched automatically by CMake during the build process if tests are enabled.

### Build Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/YOUR_ORG/YOUR_REPO.git # Replace with actual URL
    cd YOUR_REPO # YOUR_REPO should be the name of the cloned directory
    ```

2.  **Create a build directory and run CMake:**
    It's best practice to build outside the source tree.
    ```bash
    mkdir build
    cd build
    cmake .. 
    ```
    You can pass standard CMake options, e.g., `-DCMAKE_BUILD_TYPE=Release` or `-DCMAKE_INSTALL_PREFIX=/your/install/path`.

3.  **Compile the library (and tests/examples):**
    ```bash
    cmake --build . --config Release # Or Debug, etc.
    ```
    Alternatively, using `make` on Linux/macOS (after `cmake ..`):
    ```bash
    make -j$(nproc) # Adjust $(nproc) or use a specific number of cores
    ```
    Or using Ninja (if configured with `cmake -G Ninja ..`):
    ```bash
    ninja
    ```

The compiled library (e.g., `libredisjson++.a` or `libredisjson++.so` / `redisjson++.dll`) and executables (tests, examples) will be located in the build directory.

## Running Tests

Tests are written using GoogleTest and can be run using CTest after building the project.

1.  **Build the project** with tests enabled (default behavior, see `CMakeLists.txt`).
2.  **Run tests from the build directory:**
    ```bash
    cd build # If not already there
    ctest
    ```
    For more detailed output:
    ```bash
    ctest --verbose
    ```
    You can also run the test executable directly (e.g., `build/unit_tests` or `build/bin/unit_tests` depending on CMake setup).

## Dependencies

- **hiredis**: Core Redis C client library. Found via `pkg-config`.
- **nlohmann/json**: JSON library for C++. Vendored in `thirdparty/`.
- **GoogleTest**: For unit and integration testing. Fetched via CMake's `FetchContent`.

## Contributing

Contributions are welcome! If you'd like to contribute, please follow these general guidelines:
1.  Fork the repository.
2.  Create a new branch for your feature or bug fix (`git checkout -b feature/my-new-feature` or `bugfix/issue-number`).
3.  Make your changes. Adhere to the existing code style.
4.  Add unit tests for any new functionality or bug fixes.
5.  Ensure all tests pass (`ctest` in your build directory).
6.  Commit your changes with clear, descriptive commit messages.
7.  Push your branch to your fork.
8.  Submit a pull request to the main repository.

Please check if a `CONTRIBUTING.md` file exists for more detailed guidelines.

## License

This project is licensed under the **MIT License**.
A copy of the MIT License is typically included in a `LICENSE` or `LICENSE.md` file in the root of the repository. (The `docs/requirement.md` specifies MIT License).

---

## Project Structure

The repository is organized as follows:

- **`.github/`**: Contains GitHub Actions workflow configurations for CI.
- **`CMakeLists.txt`**: The main CMake build script for the project.
- **`README.md`**: This file, providing an overview of the project.
- **`README_swss.md`**: Specific README for SWSS integration.
- **`docs/`**: Contains detailed documentation.
  - `design.md`: Software design document.
  - `lua_design.md`: Design details for Lua scripting.
  - `requirement.md`: Project requirements and API specifications.
- **`examples/`**: Contains sample code demonstrating how to use the library.
  - `sample.cpp`: A general example showcasing various features of RedisJSON++.
  - `sample_swss.cpp`: An example demonstrating integration with SWSS.
- **`include/redisjson++/`**: Contains all public header files for the library. This is the primary interface for users of RedisJSON++.
- **`src/`**: Contains the C++ source code implementation of the library.
- **`tests/`**: Contains unit and integration tests for the library, built using GoogleTest.
- **`thirdparty/`**: Contains third-party dependencies included directly in the repository (e.g., `nlohmann/json.hpp`).

---

*This README provides a general overview. For more detailed information, technical specifications, and advanced features, please consult the `docs/` directory (especially `docs/requirement.md`) and the source code.*
