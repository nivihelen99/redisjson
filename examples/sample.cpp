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

void run_array_operations_extended(redisjson::RedisJSONClient& client) {
    print_header("Array Operations (Extended)");
    std::string list_key = "sample:array:ext_items";
    json initial_data = {
        {"description", "A list of numbers"},
        {"values", {10, 20, 30, 40, 50}}
    };
    client.set_json(list_key, initial_data);
    std::cout << "Setup: Initial array for extended operations '" << list_key << "':\n" << initial_data.dump(2) << std::endl;

    // 1. Iterate through array elements
    try {
        std::cout << "\nIterating through 'values' array:" << std::endl;
        json current_array = client.get_path(list_key, "values");
        if (current_array.is_array()) {
            for (const auto& item : current_array) {
                std::cout << "- Item: " << item.dump() << std::endl;
            }
        } else {
            std::cout << "ERROR: 'values' is not an array." << std::endl;
        }
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Iterating array: " << e.what() << std::endl;
    }

    // 2. Get element at a particular index
    try {
        std::cout << "\nGetting element at index 2 of 'values':" << std::endl;
        json element = client.get_path(list_key, "values[2]"); // Get the third element
        std::cout << "SUCCESS: Element at index 2: " << element.dump() << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Getting element at index: " << e.what() << std::endl;
    }

    // 3. Remove an element from a particular index
    try {
        std::cout << "\nRemoving element at index 1 from 'values':" << std::endl;
        // Current array: [10, 20, 30, 40, 50]
        // Element at index 1 is 20
        json removed_element = client.pop_path(list_key, "values", 1);
        std::cout << "SUCCESS: Removed element: " << removed_element.dump() << std::endl;
        json current_doc = client.get_json(list_key);
        std::cout << "Document after removing element at index 1:\n" << current_doc.dump(2) << std::endl;
        // Expected array: [10, 30, 40, 50]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Removing element at index: " << e.what() << std::endl;
    }
     // 4. Get array length (verify after removal)
    try {
        size_t len = client.array_length(list_key, "values");
        std::cout << "\nSUCCESS: Array length of 'values' after removal: " << len << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Array length: " << e.what() << std::endl;
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
        json old_value = client.non_atomic_get_set(atomic_key, "value", 10);
        std::cout << "SUCCESS: ATOMIC_GET_SET on 'value'. Old value: " << old_value.dump()
                  << ", New value: " << client.get_path(atomic_key, "value").dump() << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: ATOMIC_GET_SET: " << e.what()
                  << " (This might indicate Lua script not found/failed or non-atomic fallback issues)" << std::endl;
    }

    // 2. Atomic Compare and Set
    try {
        bool success = client.non_atomic_compare_set(atomic_key, "version", 1, 2);
        std::cout << "\nSUCCESS: ATOMIC_COMPARE_SET on 'version' (expected 1, new 2). Success: "
                  << (success ? "true" : "false") << std::endl;
        std::cout << "Current 'version': " << client.get_path(atomic_key, "version").dump() << std::endl;

        success = client.non_atomic_compare_set(atomic_key, "version", 1, 5); // This should fail
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

void run_sparse_merge_operations(redisjson::RedisJSONClient& client) {
    print_header("Sparse Merge Operations (set_json_sparse)");
    std::string merge_key = "sample:sparse:user_settings";

    // 1. Initial document setup
    json initial_settings = {
        {"username", "biff_carson"},
        {"theme", "light"},
        {"notifications", {
            {"email", true},
            {"sms", false}
        }},
        {"language", "en"}
    };
    try {
        client.set_json(merge_key, initial_settings);
        std::cout << "Setup: Initial document for key '" << merge_key << "':\n"
                  << client.get_json(merge_key).dump(2) << std::endl;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: Initial SET for sparse merge demo: " << e.what() << std::endl;
        return; // Cannot proceed if setup fails
    }

    // 2. Perform a sparse merge to update and add fields
    json sparse_update = {
        {"theme", "dark"}, // Update existing
        {"notifications", { // Overwrite nested object
            {"email", true},
            {"sms", true},
            {"push", false}
        }},
        {"new_feature_flag", true} // Add new field
    };
    try {
        bool success = client.set_json_sparse(merge_key, sparse_update);
        std::cout << "\nSUCCESS: Called set_json_sparse. Result: " << (success ? "true" : "false") << std::endl;
        std::cout << "Document after sparse merge:\n"
                  << client.get_json(merge_key).dump(2) << std::endl;
        // Verification:
        // - theme should be "dark"
        // - language should still be "en"
        // - notifications should be the new object
        // - new_feature_flag should be true
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: set_json_sparse on existing key: " << e.what() << std::endl;
    }

    // 3. Perform sparse merge on a non-existent key (should create it)
    std::string new_merge_key = "sample:sparse:new_doc";
    json new_doc_data = {
        {"service_name", "alpha_service"},
        {"status", "pending"}
    };
    try {
        // Ensure key is deleted if it exists from a previous run
        try { client.del_json(new_merge_key); } catch (...) {}

        bool success = client.set_json_sparse(new_merge_key, new_doc_data);
        std::cout << "\nSUCCESS: Called set_json_sparse on new key '" << new_merge_key << "'. Result: " << (success ? "true" : "false") << std::endl;
        std::cout << "Document after sparse merge on new key:\n"
                  << client.get_json(new_merge_key).dump(2) << std::endl;
        if (client.get_json(new_merge_key) != new_doc_data) {
            std::cerr << "VERIFICATION ERROR: Document created by sparse merge differs from input!" << std::endl;
        }
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR: set_json_sparse on new key: " << e.what() << std::endl;
    }

    // 4. Attempt sparse merge with non-object input (should fail or be handled by client method)
    json non_object_input = json::array({"this", "is", "not", "an", "object"});
    try {
        std::cout << "\nAttempting set_json_sparse with non-object input (expected to fail):" << std::endl;
        client.set_json_sparse(merge_key, non_object_input);
        std::cerr << "ERROR: set_json_sparse with non-object input did not throw as expected." << std::endl;
    } catch (const redisjson::ArgumentInvalidException& e) { // Corrected Type
        std::cout << "SUCCESS: Caught expected ArgumentInvalidException for non-object input: " << e.what() << std::endl;
    } catch (const redisjson::RedisJSONException& e) { // General fallback
        std::cout << "SUCCESS: Caught RedisJSONException for non-object input: " << e.what() << std::endl;
    }


    // 5. Attempt sparse merge into an existing array (should fail)
    std::string array_key = "sample:sparse:existing_array";
    try {
        client.set_json(array_key, json::array({"one", "two"}));
        std::cout << "\nAttempting set_json_sparse into an existing array (expected to fail):" << std::endl;
        client.set_json_sparse(array_key, {{"field", "value"}});
        std::cerr << "ERROR: set_json_sparse into an existing array did not throw as expected." << std::endl;
    } catch (const redisjson::LuaScriptException& e) { // Corrected Type: Lua script will error out
         std::cout << "SUCCESS: Caught expected LuaScriptException for merge into array: " << e.what() << std::endl;
    } catch (const redisjson::RedisJSONException& e) { // Broader catch
         std::cout << "SUCCESS: Caught RedisJSONException for merge into array: " << e.what() << std::endl;
    }


    // Cleanup
    try { client.del_json(merge_key); } catch (...) {}
    try { client.del_json(new_merge_key); } catch (...) {}
    try { client.del_json(array_key); } catch (...) {}
}

void run_object_operations(redisjson::RedisJSONClient& client) {
    print_header("Object Operations (OBJKEYS)");
    std::string obj_key = "sample:object:user_prefs";
    json user_preferences = {
        {"username", "gamer123"},
        {"theme", "dark"},
        {"notifications", {
            {"email", true},
            {"sms", false},
            {"push", true}
        }},
        {"language", "en-US"},
        {"empty_obj", json::object()}
    };

    try {
        client.set_json(obj_key, user_preferences);
        std::cout << "Setup: Initial object document set for key '" << obj_key << "':\n"
                  << client.get_json(obj_key).dump(2) << std::endl;

        // 1. Get keys from the root object
        std::vector<std::string> root_keys = client.object_keys(obj_key);
        std::cout << "\nSUCCESS: Keys at root '$' of '" << obj_key << "':" << std::endl;
        if (root_keys.empty()) {
            std::cout << "  (No keys found or target is not an object)" << std::endl;
        } else {
            for (const auto& k : root_keys) {
                std::cout << "  - " << k << std::endl;
            }
        }
        // For verification, sort and check
        std::vector<std::string> expected_root_keys = {"username", "theme", "notifications", "language", "empty_obj"};
        std::sort(root_keys.begin(), root_keys.end());
        std::sort(expected_root_keys.begin(), expected_root_keys.end());
        if (root_keys != expected_root_keys) {
            std::cerr << "VERIFICATION ERROR: Root keys do not match expected." << std::endl;
        }


        // 2. Get keys from a nested object
        std::vector<std::string> notification_keys = client.object_keys(obj_key, "notifications");
        std::cout << "\nSUCCESS: Keys at path 'notifications' of '" << obj_key << "':" << std::endl;
        if (notification_keys.empty()) {
            std::cout << "  (No keys found or target is not an object)" << std::endl;
        } else {
            for (const auto& k : notification_keys) {
                std::cout << "  - " << k << std::endl;
            }
        }
        std::vector<std::string> expected_notification_keys = {"email", "sms", "push"};
        std::sort(notification_keys.begin(), notification_keys.end());
        std::sort(expected_notification_keys.begin(), expected_notification_keys.end());
        if (notification_keys != expected_notification_keys) {
            std::cerr << "VERIFICATION ERROR: Notification keys do not match expected." << std::endl;
        }

        // 3. Get keys from an empty nested object
        std::vector<std::string> empty_obj_keys = client.object_keys(obj_key, "empty_obj");
        std::cout << "\nSUCCESS: Keys at path 'empty_obj' of '" << obj_key << "':" << std::endl;
        if (empty_obj_keys.empty()) {
            std::cout << "  (No keys found or target is not an object - expected for empty object)" << std::endl;
        } else {
            for (const auto& k : empty_obj_keys) {
                std::cout << "  - " << k << std::endl;
            }
        }
         if (!empty_obj_keys.empty()) { // Should be empty
            std::cerr << "VERIFICATION ERROR: Keys for empty_obj should be empty." << std::endl;
        }


        // 4. Attempt to get keys from a path that is not an object (e.g., a string value)
        std::vector<std::string> string_path_keys = client.object_keys(obj_key, "theme");
        std::cout << "\nATTEMPT: Keys at path 'theme' (a string value) of '" << obj_key << "':" << std::endl;
        if (string_path_keys.empty()) {
            std::cout << "  (Correctly no keys found as 'theme' is not an object)" << std::endl;
        } else {
             std::cerr << "VERIFICATION ERROR: Keys for 'theme' (string) should be empty." << std::endl;
            for (const auto& k : string_path_keys) {
                std::cout << "  - " << k << std::endl;
            }
        }

        // 5. Attempt to get keys from a non-existent path
        std::vector<std::string> non_existent_path_keys = client.object_keys(obj_key, "settings.advanced");
        std::cout << "\nATTEMPT: Keys at non-existent path 'settings.advanced' of '" << obj_key << "':" << std::endl;
        if (non_existent_path_keys.empty()) {
            std::cout << "  (Correctly no keys found as path does not exist)" << std::endl;
        } else {
            std::cerr << "VERIFICATION ERROR: Keys for non-existent path should be empty." << std::endl;
        }


    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR in Object Operations: " << e.what() << std::endl;
    }

    // Cleanup
    try { client.del_json(obj_key); } catch (...) {}
}

void run_numeric_operations(redisjson::RedisJSONClient& client) {
    print_header("Numeric Operations (JSON.NUMINCRBY)");
    std::string num_key = "sample:numeric:data";
    json initial_data = {
        {"id", "counter_set_1"},
        {"values", {
            {"active_users", 100},
            {"total_requests", 5000.5}
        }},
        {"non_numeric", "text"}
    };

    try {
        client.set_json(num_key, initial_data);
        std::cout << "Setup: Initial numeric document set for key '" << num_key << "':\n"
                  << client.get_json(num_key).dump(2) << std::endl;

        // 1. Increment an integer value
        json new_active_users = client.json_numincrby(num_key, "values.active_users", 5);
        std::cout << "\nSUCCESS: Incremented 'values.active_users' by 5. New value: " << new_active_users.dump() << std::endl;
        // Verification
        if (new_active_users.get<double>() != 105) { // Lua returns numbers, nlohmann might parse as double
            std::cerr << "VERIFICATION ERROR: 'values.active_users' should be 105, got " << new_active_users.dump() << std::endl;
        }

        // 2. Increment a floating point value
        json new_total_requests = client.json_numincrby(num_key, "values.total_requests", 100.25);
        std::cout << "SUCCESS: Incremented 'values.total_requests' by 100.25. New value: " << new_total_requests.dump() << std::endl;
         // Verification (floating point comparison)
        if (std::abs(new_total_requests.get<double>() - 5100.75) > 0.001) {
             std::cerr << "VERIFICATION ERROR: 'values.total_requests' should be approx 5100.75, got " << new_total_requests.dump() << std::endl;
        }


        // 3. Decrement an integer value (using negative increment)
        new_active_users = client.json_numincrby(num_key, "values.active_users", -10);
        std::cout << "SUCCESS: Decremented 'values.active_users' by 10. New value: " << new_active_users.dump() << std::endl;
        if (new_active_users.get<double>() != 95) {
             std::cerr << "VERIFICATION ERROR: 'values.active_users' should be 95, got " << new_active_users.dump() << std::endl;
        }

        std::cout << "\nFinal document state for key '" << num_key << "':\n"
                  << client.get_json(num_key).dump(2) << std::endl;

        // 4. Attempt to increment a non-numeric field (expected to fail)
        std::cout << "\nATTEMPT: Increment 'non_numeric' (a string value) - Expected to fail:" << std::endl;
        try {
            client.json_numincrby(num_key, "non_numeric", 5);
            std::cerr << "ERROR: json_numincrby on non-numeric field did not throw as expected." << std::endl;
        } catch (const redisjson::LuaScriptException& e) {
            std::cout << "SUCCESS: Caught expected LuaScriptException for NUMINCRBY on non-numeric: " << e.what() << std::endl;
        } catch (const redisjson::TypeMismatchException& e) { // For SWSS client-side check
             std::cout << "SUCCESS: Caught expected TypeMismatchException for NUMINCRBY on non-numeric (SWSS mode): " << e.what() << std::endl;
        }


        // 5. Attempt to increment on a non-existent path (expected to fail)
         std::cout << "\nATTEMPT: Increment 'values.new_counter' (non-existent path) - Expected to fail:" << std::endl;
        try {
            client.json_numincrby(num_key, "values.new_counter", 10);
            std::cerr << "ERROR: json_numincrby on non-existent path did not throw as expected." << std::endl;
        } catch (const redisjson::LuaScriptException& e) { // Lua script returns ERR_NOPATH
            std::cout << "SUCCESS: Caught expected LuaScriptException for NUMINCRBY on non-existent path: " << e.what() << std::endl;
        } catch (const redisjson::PathNotFoundException& e) { // For SWSS client-side check
             std::cout << "SUCCESS: Caught expected PathNotFoundException for NUMINCRBY on non-existent path (SWSS mode): " << e.what() << std::endl;
        }


        // 6. Attempt to increment on a non-existent key (expected to fail)
        std::cout << "\nATTEMPT: Increment on 'sample:numeric:non_existent_key' - Expected to fail:" << std::endl;
        try {
            client.json_numincrby("sample:numeric:non_existent_key", "counter", 1);
            std::cerr << "ERROR: json_numincrby on non-existent key did not throw as expected." << std::endl;
        } catch (const redisjson::LuaScriptException& e) { // Lua script returns ERR_NOKEY
            std::cout << "SUCCESS: Caught expected LuaScriptException for NUMINCRBY on non-existent key: " << e.what() << std::endl;
        } catch (const redisjson::PathNotFoundException& e) { // For SWSS client-side check (get_json fails)
             std::cout << "SUCCESS: Caught expected PathNotFoundException for NUMINCRBY on non-existent key (SWSS mode): " << e.what() << std::endl;
        }


    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR in Numeric Operations: " << e.what() << std::endl;
    }

    // Cleanup
    try { client.del_json(num_key); } catch (...) {}
}


int main() {
    // --- Non-SWSS Mode / Legacy Mode Example ---
    std::cout << "--- Running in Non-SWSS (Legacy) Mode ---" << std::endl;

    redisjson::LegacyClientConfig legacy_config;
    const char* redis_host_env = std::getenv("REDIS_HOST");
    if (redis_host_env) legacy_config.host = redis_host_env;
    const char* redis_port_env = std::getenv("REDIS_PORT");
    if (redis_port_env) try { legacy_config.port = std::stoi(redis_port_env); } catch (...) {}
    const char* redis_password_env = std::getenv("REDIS_PASSWORD");
    if (redis_password_env) legacy_config.password = redis_password_env;

    // Default to localhost if not set by environment variables
    if (legacy_config.host.empty()) {
        legacy_config.host = "127.0.0.1";
    }
    // Default port if not set or invalid
    if (legacy_config.port == 0) {
         legacy_config.port = 6379;
    }


    std::cout << "Attempting to connect to Non-SWSS Redis at " << legacy_config.host << ":" << legacy_config.port << std::endl;
    try {
        redisjson::RedisJSONClient legacy_client(legacy_config);
        std::cout << "RedisJSONClient (Non-SWSS Mode) initialized successfully." << std::endl;

        run_document_operations(legacy_client);
        run_path_operations(legacy_client);
        run_array_operations(legacy_client);
        run_array_operations_extended(legacy_client); // Added extended array operations
        run_atomic_operations(legacy_client); // Original atomic operations using Lua
        run_sparse_merge_operations(legacy_client); // <--- Added new demo
        run_object_operations(legacy_client); // <--- Added new demo for OBJKEYS
        run_numeric_operations(legacy_client); // <--- Added new demo for NUMINCRBY

        print_header("Non-SWSS (Legacy) Mode Sample Program Finished");

    } catch (const redisjson::ConnectionException& e) {
        std::cerr << "CRITICAL (Non-SWSS): Could not connect to Redis. " << e.what() << std::endl;
        std::cerr << "Ensure Redis is running at " << legacy_config.host << ":" << legacy_config.port
                  << " or set REDIS_HOST/REDIS_PORT environment variables." << std::endl;
        return 1;
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "CRITICAL (Non-SWSS): A RedisJSON++ error occurred: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "CRITICAL (Non-SWSS): A standard C++ exception occurred: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

/*
To compile and run this sample (assuming RedisJSON++ is installed or built alongside):

1. Save this file as `sample.cpp` (for Non-SWSS mode) in an `examples` directory.
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
