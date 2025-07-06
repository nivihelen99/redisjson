#include "redisjson++/redis_json_client.h"
#include "redisjson++/common_types.h"
#include "redisjson++/exceptions.h"
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <cstdlib> // For std::getenv

// Alias for convenience
using json = nlohmann::json;

void print_header(const std::string& header) {
    std::cout << "\n--- " << header << " ---\n" << std::endl;
}

void run_document_operations(redisjson::RedisJSONClient& client) {
    print_header("Document Operations");
    std::string doc_key = "sample:doc:user1";
    json user_profile = {
        {"name", "John Doe"},
        {"email", "john.doe@example.com"},
        {"age", 30},
        {"isVerified", true},
        {"address", {
            {"street", "123 Main St"},
            {"city", "Anytown"}
        }},
        {"hobbies", {"reading", "cycling", "photography"}}
    };

    // 1. Set JSON document
    try {
        client.set_json(doc_key, user_profile);
        std::cout << "SUCCESS: SET document for key '" << doc_key << "'." << std::endl;
        // std::cout << user_profile.dump(2) << std::endl; // Verify content if needed
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: SET document: " << e.what() << std::endl;
    }

    // 2. Get JSON document
    try {
        json retrieved_doc = client.get_json(doc_key);
        std::cout << "\nSUCCESS: GET document for key '" << doc_key << "':" << std::endl;
        std::cout << retrieved_doc.dump(2) << std::endl;
        if (retrieved_doc != user_profile) {
             std::cerr << "VERIFICATION ERROR: Retrieved document differs from original!" << std::endl;
        }
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: GET document: " << e.what() << std::endl;
    }

    // 3. Check if JSON document exists
    std::cout << "\nChecking existence:" << std::endl;
    bool exists = client.exists_json(doc_key);
    std::cout << "SUCCESS: Key '" << doc_key << "' exists: " << (exists ? "true" : "false") << std::endl;
    bool not_exists = client.exists_json("sample:doc:nonexistent");
    std::cout << "SUCCESS: Key 'sample:doc:nonexistent' exists: " << (not_exists ? "true" : "false") << std::endl;

    // 4. Delete JSON document
    try {
        client.del_json(doc_key);
        std::cout << "\nSUCCESS: DEL document for key '" << doc_key << "'." << std::endl;
        exists = client.exists_json(doc_key);
        std::cout << "SUCCESS: Key '" << doc_key << "' exists after delete: " << (exists ? "true" : "false") << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: DEL document: " << e.what() << std::endl;
    }
}

void run_path_operations(redisjson::RedisJSONClient& client) {
    print_header("Path Operations");
    std::string user_key = "sample:path:user2";
    json user_data = {
        {"name", "Jane Smith"},
        {"contact", {
            {"email", "jane.smith@example.com"},
            {"phone", "555-1234"}
        }},
        {"preferences", {
            {"theme", "dark"},
            {"notifications", json::array({"email", "sms"})}
        }},
        {"status", "active"}
    };
    client.set_json(user_key, user_data);
    std::cout << "Setup: Initial document set for key '" << user_key << "':\n" << user_data.dump(2) << std::endl;

    // 1. Get value at path
    try {
        json email = client.get_path(user_key, "contact.email");
        std::cout << "SUCCESS: GET path 'contact.email': " << email.dump() << std::endl;
        json first_notification = client.get_path(user_key, "preferences.notifications[0]");
        std::cout << "SUCCESS: GET path 'preferences.notifications[0]': " << first_notification.dump() << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: GET path: " << e.what() << std::endl;
    }

    // 2. Set value at path
    try {
        client.set_path(user_key, "status", "inactive");
        std::cout << "\nSUCCESS: SET path 'status' to 'inactive'." << std::endl;
        client.set_path(user_key, "contact.phone", "555-5678"); // Overwrite existing
        std::cout << "SUCCESS: SET path 'contact.phone' to '555-5678'." << std::endl;
        client.set_path(user_key, "profile.lastLogin", "2024-07-26T10:00:00Z"); // Create intermediate path
        std::cout << "SUCCESS: SET path 'profile.lastLogin' (created path)." << std::endl;

        json current_doc = client.get_json(user_key);
        std::cout << "Current document after SET path operations:\n" << current_doc.dump(2) << std::endl;

    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: SET path: " << e.what() << std::endl;
    }

    // 3. Check path existence
    std::cout << "\nChecking path existence:" << std::endl;
    bool path_exists = client.exists_path(user_key, "contact.phone");
    std::cout << "SUCCESS: Path 'contact.phone' exists: " << (path_exists ? "true" : "false") << std::endl;
    bool path_not_exists = client.exists_path(user_key, "contact.fax");
    std::cout << "SUCCESS: Path 'contact.fax' exists: " << (path_not_exists ? "true" : "false") << std::endl;
    bool root_path_exists = client.exists_path(user_key, "$");
    std::cout << "SUCCESS: Path '$' (root) exists: " << (root_path_exists ? "true" : "false") << std::endl;


    // 4. Delete value at path
    try {
        client.del_path(user_key, "preferences.theme");
        std::cout << "\nSUCCESS: DEL path 'preferences.theme'." << std::endl;
        json current_doc = client.get_json(user_key);
        std::cout << "Current document after DEL path operation:\n" << current_doc.dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: DEL path: " << e.what() << std::endl;
    }

    // Cleanup
    client.del_json(user_key);
}

void run_array_operations(redisjson::RedisJSONClient& client) {
    print_header("Array Operations");
    std::string list_key = "sample:array:items";
    json initial_list = {
        {"id", "list1"},
        {"items", {"apples", "bananas"}}
    };
    client.set_json(list_key, initial_list);
    std::cout << "Setup: Initial array document set for key '" << list_key << "':\n" << initial_list.dump(2) << std::endl;

    // 1. Append to array
    try {
        client.append_path(list_key, "items", "cherries");
        std::cout << "SUCCESS: APPEND 'cherries' to 'items'." << std::endl;
        json current_doc = client.get_json(list_key);
        std::cout << "Document after append:\n" << current_doc.dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: APPEND to array: " << e.what() << std::endl;
    }

    // 2. Prepend to array
    try {
        client.prepend_path(list_key, "items", "elderberries");
        std::cout << "\nSUCCESS: PREPEND 'elderberries' to 'items'." << std::endl;
        json current_doc = client.get_json(list_key);
        std::cout << "Document after prepend:\n" << current_doc.dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: PREPEND to array: " << e.what() << std::endl;
    }

    // 3. Get array length
    try {
        size_t len = client.array_length(list_key, "items");
        std::cout << "\nSUCCESS: Array length of 'items': " << len << std::endl; // Should be 4
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Array length: " << e.what() << std::endl;
    }

    // 4. Pop from array (last element)
    try {
        json popped_val = client.pop_path(list_key, "items"); // Default index -1
        std::cout << "\nSUCCESS: POP from 'items' (last element): " << popped_val.dump() << std::endl;
        json current_doc = client.get_json(list_key);
        std::cout << "Document after pop (last):\n" << current_doc.dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: POP from array (last): " << e.what() << std::endl;
    }

    // 5. Pop from array (first element)
    try {
        json popped_val = client.pop_path(list_key, "items", 0);
        std::cout << "\nSUCCESS: POP from 'items' (index 0): " << popped_val.dump() << std::endl;
        json current_doc = client.get_json(list_key);
        std::cout << "Document after pop (index 0):\n" << current_doc.dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: POP from array (index 0): " << e.what() << std::endl;
    }

    // Cleanup
    client.del_json(list_key);
}

void run_atomic_operations(redisjson::RedisJSONClient& client) {
    print_header("Atomic Operations (Conceptual - Requires Lua Scripts)");
    std::string atomic_key = "sample:atomic:counter";
    client.set_json(atomic_key, { {"value", 0}, {"version", 1} });
    std::cout << "Setup: Initial atomic document set for key '" << atomic_key << "':\n"
              << client.get_json(atomic_key).dump(2) << std::endl;

    // Note: These operations rely on Lua scripts being correctly implemented and loaded.
    // The default build of this library might use non-atomic Get-Modify-Set if Lua scripts are missing.
    // For this example, we assume they are conceptually atomic.

    // 1. Atomic Get and Set
    try {
        json old_value = client.atomic_get_set(atomic_key, "value", 10);
        std::cout << "SUCCESS: ATOMIC_GET_SET on 'value'. Old value: " << old_value.dump()
                  << ", New value: " << client.get_path(atomic_key, "value").dump() << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: ATOMIC_GET_SET: " << e.what()
                  << " (This might indicate Lua script not found/failed or non-atomic fallback issues)" << std::endl;
    }

    // 2. Atomic Compare and Set
    try {
        bool success = client.atomic_compare_set(atomic_key, "version", 1, 2);
        std::cout << "\nSUCCESS: ATOMIC_COMPARE_SET on 'version' (expected 1, new 2). Success: "
                  << (success ? "true" : "false") << std::endl;
        std::cout << "Current 'version': " << client.get_path(atomic_key, "version").dump() << std::endl;

        success = client.atomic_compare_set(atomic_key, "version", 1, 5); // This should fail
        std::cout << "\nSUCCESS: ATOMIC_COMPARE_SET on 'version' (expected 1, new 5). Success: "
                  << (success ? "true" : "false") << std::endl;
        std::cout << "Current 'version': " << client.get_path(atomic_key, "version").dump() << std::endl;

    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: ATOMIC_COMPARE_SET: " << e.what()
                  << " (This might indicate Lua script not found/failed or non-atomic fallback issues)" << std::endl;
    }

    // Cleanup
    client.del_json(atomic_key);
}


int main() {
    // --- SWSS Mode Example ---
    std::cout << "--- Running in SWSS Mode ---" << std::endl;
    redisjson::SwssClientConfig swss_config;
    // Typically in SONiC, DBConnector connects to a specific DB, e.g., APPL_DB.
    // The DB number for APPL_DB is often 0, but using names is safer if DBConnector supports it.
    // For this example, let's assume we want to use a database name.
    // If your DBConnector primarily uses integer IDs, you'd initialize it differently.
    swss_config.db_name = "APPL_DB"; // Or "STATE_DB", "CONFIG_DB", etc.
                                     // Or an integer if DBConnector uses int: swss_config.db_id = 0;
    swss_config.unix_socket_path = "/var/run/redis/redis.sock"; // Standard SONiC path
    swss_config.operation_timeout_ms = 5000;
    swss_config.wait_for_db = false; // Set to true if the client should wait for DB availability

    std::cout << "Attempting to connect to SWSS DB: " << swss_config.db_name
              << " via " << swss_config.unix_socket_path << std::endl;

    try {
        redisjson::RedisJSONClient swss_client(swss_config);
        std::cout << "RedisJSONClient (SWSS Mode) initialized successfully for DB: " << swss_config.db_name << std::endl;

        // Before running operations, it's good practice to flush the test DB if possible,
        // or ensure keys are unique to avoid interference from previous runs.
        // Note: DBConnector might not expose a flushdb() method directly.
        // For a real SONiC app, you wouldn't typically flush APPL_DB.
        // This is more for isolated testing.
        // For this example, we'll rely on unique keys or manual cleanup.

        run_document_operations(swss_client);
        run_path_operations(swss_client); // Note: atomicity of path ops is lost in SWSS mode
        run_array_operations(swss_client);  // Note: atomicity of array ops is lost in SWSS mode

        // Rename atomic operations to reflect their non-atomic nature in SWSS mode.
        // The functions in the client are already renamed to non_atomic_...
        // The example function `run_atomic_operations` needs to call these.
        // For clarity, we can rename the example function too or add comments.
        print_header("Non-Atomic Operations (SWSS Mode - atomicity lost)");
        // Create a temporary key for these non-atomic tests
        std::string non_atomic_key = "sample:non_atomic:swss_counter";
        swss_client.set_json(non_atomic_key, { {"value", 0}, {"version", 1} });
        std::cout << "Setup: Initial non-atomic document set for key '" << non_atomic_key << "':\n"
                  << swss_client.get_json(non_atomic_key).dump(2) << std::endl;
        try {
            json old_val = swss_client.non_atomic_get_set(non_atomic_key, "value", 100);
            std::cout << "SUCCESS: NON_ATOMIC_GET_SET on 'value'. Old value: " << old_val.dump()
                      << ", New value: " << swss_client.get_path(non_atomic_key, "value").dump() << std::endl;

            bool cas_success = swss_client.non_atomic_compare_set(non_atomic_key, "version", 1, 20);
             std::cout << "\nSUCCESS: NON_ATOMIC_COMPARE_SET on 'version' (expected 1, new 20). Success: "
                      << (cas_success ? "true" : "false") << std::endl;
            std::cout << "Current 'version': " << swss_client.get_path(non_atomic_key, "version").dump() << std::endl;

        } catch (const redisjson::RedisJSONException& e) {
            std::cerr << "ERROR: Non-atomic operation failed (SWSS): " << e.what() << std::endl;
        }
        swss_client.del_json(non_atomic_key);


        print_header("SWSS Mode Sample Program Finished");

    } catch (const redisjson::ConnectionException& e) {
        std::cerr << "CRITICAL (SWSS): Could not connect to Redis DB '" << swss_config.db_name << "'. " << e.what() << std::endl;
        std::cerr << "Please ensure Redis is running and accessible via " << swss_config.unix_socket_path
                  << " and the DB name/ID is correct." << std::endl;
        return 1;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "CRITICAL (SWSS): A RedisJSON++ error occurred: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "CRITICAL (SWSS): A standard C++ exception occurred: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n\n--- Running in Legacy Mode (if configured) ---" << std::endl;
    // --- Legacy Mode Example (original logic) ---
    // This part would use LegacyClientConfig and connect directly to a Redis host/port.
    // For CI/testing, this might still be useful.
    // You would typically guard this with a compile-time or run-time flag.
    bool run_legacy_example = false; // Set to true to run legacy example
    const char* run_legacy_env = std::getenv("RUN_REDISJSON_LEGACY_EXAMPLE");
    if (run_legacy_env && std::string(run_legacy_env) == "1") {
        run_legacy_example = true;
    }

    if (run_legacy_example) {
        redisjson::LegacyClientConfig legacy_config;
        const char* redis_host_env = std::getenv("REDIS_HOST");
        if (redis_host_env) legacy_config.host = redis_host_env;
        const char* redis_port_env = std::getenv("REDIS_PORT");
        if (redis_port_env) try { legacy_config.port = std::stoi(redis_port_env); } catch (...) {}
        const char* redis_password_env = std::getenv("REDIS_PASSWORD");
        if (redis_password_env) legacy_config.password = redis_password_env;

        std::cout << "Attempting to connect to Legacy Redis at " << legacy_config.host << ":" << legacy_config.port << std::endl;
        try {
            redisjson::RedisJSONClient legacy_client(legacy_config);
            std::cout << "RedisJSONClient (Legacy Mode) initialized successfully." << std::endl;

            run_document_operations(legacy_client);
            run_path_operations(legacy_client);
            run_array_operations(legacy_client);
            run_atomic_operations(legacy_client); // Original atomic operations using Lua

            print_header("Legacy Mode Sample Program Finished");

        } catch (const redisjson::ConnectionException& e) {
            std::cerr << "CRITICAL (Legacy): Could not connect to Redis. " << e.what() << std::endl;
        } catch (const redisjson::RedisJSONException& e) {
            std::cerr << "CRITICAL (Legacy): A RedisJSON++ error occurred: " << e.what() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "CRITICAL (Legacy): A standard C++ exception occurred: " << e.what() << std::endl;
        }
    } else {
        std::cout << "Legacy mode example skipped. Set RUN_REDISJSON_LEGACY_EXAMPLE=1 to run." << std::endl;
    }
        return 1;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "CRITICAL: A RedisJSON++ error occurred: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "CRITICAL: A standard C++ exception occurred: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

/*
To compile and run this sample (assuming RedisJSON++ is installed or built alongside):

1. Save this file as `sample.cpp` in an `examples` directory.
2. Ensure RedisJSON++ library and its dependencies (hiredis, nlohmann/json) are findable by CMake.

Example CMakeLists.txt for this sample (place in `examples/CMakeLists.txt`):

```cmake
cmake_minimum_required(VERSION 3.16)
project(RedisJSONSample)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# This assumes the main library (RedisJSONPlusPlus) is in the parent directory.
# If RedisJSONPlusPlus is installed, you might use find_package instead.
# If it's fetched via FetchContent in a parent CMakeLists.txt, it should already be available.

# If this CMakeLists.txt is processed by a parent CMakeLists.txt that already handles dependencies:
# You might not need to find_package for hiredis and nlohmann_json again here.
# However, if building this example standalone (e.g. for testing), you would.

# For a standalone example build, you might need to find or fetch dependencies:
# include(FetchContent)
# FetchContent_Declare(hiredis GIT_REPOSITORY https://github.com/redis/hiredis.git GIT_TAG v1.2.0)
# FetchContent_MakeAvailable(hiredis)
# FetchContent_Declare(nlohmann_json GIT_REPOSITORY https://github.com/nlohmann/json.git GIT_TAG v3.11.3)
# FetchContent_MakeAvailable(nlohmann_json)

# Link to the main library target from the parent build.
# The target RedisJSONPlusPlus::RedisJSONPlusPlus should be globally available
# if the parent CMakeLists.txt used add_subdirectory or FetchContent_MakeAvailable.

add_executable(sample_program sample.cpp)

target_link_libraries(sample_program PRIVATE
    RedisJSONPlusPlus::RedisJSONPlusPlus
    # Dependencies like hiredis and nlohmann_json are often linked transitively
    # by RedisJSONPlusPlus::RedisJSONPlusPlus if it's configured correctly.
    # If not, you might need to add them explicitly:
    # redisjson::hiredis # Or however hiredis target is named
    # nlohmann_json::nlohmann_json # Or however nlohmann_json target is named
)

# To make this example buildable from the main CMakeLists.txt, add:
# add_subdirectory(examples)
# in the parent CMakeLists.txt after RedisJSONPlusPlus is available.
```

Build steps (from the root of the RedisJSON++ project):
mkdir build
cd build
cmake ..
cmake --build . # or make
./examples/sample_program # Or path where your build system places it, e.g., ./bin/sample_program

*/
