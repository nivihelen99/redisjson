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

void run_arrinsert_operations(redisjson::RedisJSONClient& client) {
    print_header("Array Insert Operations (JSON.ARRINSERT)");
    std::string arr_key = "sample:arrinsert:demo";

    // Initial setup
    json initial_array = {"a", "b", "e", "f"};
    client.set_json(arr_key, initial_array);
    std::cout << "Initial array: " << client.get_json(arr_key).dump() << std::endl;

    // 1. Insert single element at index 2 ("c")
    try {
        long long new_len = client.arrinsert(arr_key, "$", 2, {json("c")});
        std::cout << "SUCCESS: Inserted 'c' at index 2. New length: " << new_len << std::endl;
        std::cout << "Array after insert: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["a", "b", "c", "e", "f"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting 'c': " << e.what() << std::endl;
    }

    // 2. Insert multiple elements at index 3 ("d1", "d2")
    try {
        long long new_len = client.arrinsert(arr_key, "$", 3, {json("d1"), json("d2")});
        std::cout << "\nSUCCESS: Inserted 'd1', 'd2' at index 3. New length: " << new_len << std::endl;
        std::cout << "Array after multi-insert: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["a", "b", "c", "d1", "d2", "e", "f"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting 'd1', 'd2': " << e.what() << std::endl;
    }

    // 3. Insert at the beginning (index 0)
    client.set_json(arr_key, {"middle"}); // Reset
    try {
        long long new_len = client.arrinsert(arr_key, "$", 0, {"first"});
        std::cout << "\nSUCCESS: Inserted 'first' at index 0. New length: " << new_len << std::endl;
        std::cout << "Array after insert at 0: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["first", "middle"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting at index 0: " << e.what() << std::endl;
    }

    // 4. Insert at the end (index > length)
    client.set_json(arr_key, {"item1"}); // Reset
    try {
        long long new_len = client.arrinsert(arr_key, "$", 5, {"item_last"}); // Index 5 on array of len 1
        std::cout << "\nSUCCESS: Inserted 'item_last' at index 5 (out of bounds). New length: " << new_len << std::endl;
        std::cout << "Array after insert at end: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["item1", "item_last"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting at end (index 5): " << e.what() << std::endl;
    }

    // 5. Insert using negative index (-1, before last element)
    client.set_json(arr_key, {"x", "y", "z"}); // Reset
    try {
        long long new_len = client.arrinsert(arr_key, "$", -1, {"inserted_before_last"});
        std::cout << "\nSUCCESS: Inserted 'inserted_before_last' at index -1. New length: " << new_len << std::endl;
        std::cout << "Array after insert at -1: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["x", "y", "inserted_before_last", "z"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting at index -1: " << e.what() << std::endl;
    }

    // 6. Insert into an empty array
    client.del_json(arr_key); // Ensure it's gone or make it empty
    client.set_json(arr_key, json::array());
    try {
        long long new_len = client.arrinsert(arr_key, "$", 0, {"only_item"});
        std::cout << "\nSUCCESS: Inserted 'only_item' into empty array at index 0. New length: " << new_len << std::endl;
        std::cout << "Array after insert into empty: " << client.get_json(arr_key).dump() << std::endl; // Expected: ["only_item"]
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting into empty array: " << e.what() << std::endl;
    }

    // 7. Insert into a nested array
    json nested_doc = { {"data", { {"list", {"elem1", "elem3"} }} } };
    client.set_json(arr_key, nested_doc);
    try {
        long long new_len = client.arrinsert(arr_key, "data.list", 1, {"elem2"});
        std::cout << "\nSUCCESS: Inserted 'elem2' into nested array 'data.list' at index 1. New length: " << new_len << std::endl;
        std::cout << "Document after insert into nested array: " << client.get_json(arr_key).dump(2) << std::endl;
        // Expected: {"data": {"list": ["elem1", "elem2", "elem3"]}}
    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR inserting into nested array: " << e.what() << std::endl;
    }


    // Cleanup
    client.del_json(arr_key);
}


void run_array_operations_extended(redisjson::RedisJSONClient& client) {
    print_header("Array Operations (Extended - Pop, Length, Get Path)");
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
        {"username", "Biff Larsen"},
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

        // --- JSON.OBJLEN Examples ---
        std::cout << "\n--- JSON.OBJLEN (object_length) Examples ---" << std::endl;

        // 6. Get length of the root object
        std::optional<size_t> root_len = client.object_length(obj_key);
        if (root_len) {
            std::cout << "SUCCESS: Length of root object: " << *root_len << std::endl;
            if (*root_len != 5) std::cerr << "VERIFICATION ERROR: Root object length should be 5." << std::endl;
        } else {
            std::cerr << "ERROR: Could not get length of root object." << std::endl;
        }

        // 7. Get length of a nested object 'notifications'
        std::optional<size_t> notification_len = client.object_length(obj_key, "notifications");
        if (notification_len) {
            std::cout << "SUCCESS: Length of 'notifications' object: " << *notification_len << std::endl;
            if (*notification_len != 3) std::cerr << "VERIFICATION ERROR: 'notifications' object length should be 3." << std::endl;
        } else {
            std::cerr << "ERROR: Could not get length of 'notifications' object." << std::endl;
        }

        // 8. Get length of an empty nested object 'empty_obj'
        std::optional<size_t> empty_obj_len = client.object_length(obj_key, "empty_obj");
        if (empty_obj_len) {
            std::cout << "SUCCESS: Length of 'empty_obj' object: " << *empty_obj_len << std::endl;
            if (*empty_obj_len != 0) std::cerr << "VERIFICATION ERROR: 'empty_obj' length should be 0." << std::endl;
        } else {
            std::cerr << "ERROR: Could not get length of 'empty_obj' object." << std::endl;
        }

        // 9. Attempt to get length from a path that is not an object (e.g., a string value 'theme')
        std::cout << "\nATTEMPT: Length of path 'theme' (a string value):" << std::endl;
        try {
            std::optional<size_t> string_path_len = client.object_length(obj_key, "theme");
            if (string_path_len) {
                 std::cerr << "VERIFICATION ERROR: object_length for 'theme' (string) should be std::nullopt or throw, got " << *string_path_len << std::endl;
            } else {
                std::cout << "  (Correctly got std::nullopt as 'theme' is not an object)" << std::endl;
            }
        } catch (const redisjson::LuaScriptException& e) {
            std::cout << "  (Correctly caught LuaScriptException as 'theme' is not an object: " << e.what() << ")" << std::endl;
        } catch (const redisjson::RedisJSONException& e) { // Broader catch for other client errors
            std::cout << "  (Caught RedisJSONException as 'theme' is not an object: " << e.what() << ")" << std::endl;
        }


        // 10. Attempt to get length from a non-existent path 'settings.advanced'
        std::cout << "\nATTEMPT: Length of non-existent path 'settings.advanced':" << std::endl;
        std::optional<size_t> non_existent_path_len = client.object_length(obj_key, "settings.advanced");
        if (non_existent_path_len) {
            std::cerr << "VERIFICATION ERROR: object_length for non-existent path should be std::nullopt, got " << *non_existent_path_len << std::endl;
        } else {
            std::cout << "  (Correctly got std::nullopt as path does not exist)" << std::endl;
        }

        // 11. Attempt to get length from a path that is an array (e.g. user_preferences.hobbies if it existed)
        // Let's add an array to test this specifically.
        json temp_array_data = R"({"my_array": [1,2,3]})"_json;
        client.set_path(obj_key, "my_array_holder", temp_array_data);
        std::cout << "\nATTEMPT: Length of path 'my_array_holder.my_array' (an array):" << std::endl;
        try {
            std::optional<size_t> array_path_len = client.object_length(obj_key, "my_array_holder.my_array");
            if (array_path_len) {
                 std::cerr << "VERIFICATION ERROR: object_length for an array path should be std::nullopt or throw, got " << *array_path_len << std::endl;
            } else {
                 std::cout << "  (Correctly got std::nullopt as path points to an array)" << std::endl;
            }
        } catch (const redisjson::LuaScriptException& e) {
            std::cout << "  (Correctly caught LuaScriptException as path points to an array: " << e.what() << ")" << std::endl;
        } catch (const redisjson::RedisJSONException& e) {
             std::cout << "  (Caught RedisJSONException as path points to an array: " << e.what() << ")" << std::endl;
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
        } catch (const redisjson::LuaScriptException& e) { // Lua script returns ERR_NOKEY or ERR document not found
            std::cout << "SUCCESS: Caught expected LuaScriptException for NUMINCRBY on non-existent key: " << e.what() << std::endl;
        } catch (const redisjson::PathNotFoundException& e) { // This is what client method should throw for "ERR document not found"
             std::cout << "SUCCESS: Caught expected PathNotFoundException for NUMINCRBY on non-existent key (SWSS mode or client conversion): " << e.what() << std::endl;
        }


    } catch (const redisjson::RedisJSONException& e) {
        std::cerr << "ERROR in Numeric Operations: " << e.what() << std::endl;
    }

    // Cleanup
    try { client.del_json(num_key); } catch (...) {}
}

void run_jsonclear_operations(redisjson::RedisJSONClient& client) {
    print_header("JSON.CLEAR Operations");
    std::string clear_key = "sample:clear:data";
    long long cleared_count = 0; // Reuse this variable

    // Preparatory cleanup for the key
    try { client.del_json(clear_key); } catch(...) {}

    // 1. Clear an array at root
    json array_doc = {"a", "b", 1, 2, true, {{"nested_num", 10}}};
    client.set_json(clear_key, array_doc);
    std::cout << "Initial array document: " << client.get_json(clear_key).dump() << std::endl;
    cleared_count = client.json_clear(clear_key, "$");
    std::cout << "SUCCESS: Cleared root array. Count: " << cleared_count << std::endl;
    std::cout << "Document after clearing root array: " << client.get_json(clear_key).dump() << std::endl;
    if (client.get_json(clear_key) != json::array() || (!array_doc.empty() && cleared_count != 1) || (array_doc.empty() && cleared_count != 0) ) {
        std::cerr << "VERIFICATION ERROR: Clearing root array. Expected count 1 if not empty, 0 if empty. Got: " << cleared_count << std::endl;
    }
    client.del_json(clear_key);

    // 2. Clear an object at root
    json object_doc = {
        {"name", "test"},
        {"count", 100},
        {"active", true},
        {"misc_null", nullptr},
        {"nested_obj", {
            {"value", 200},
            {"sub_arr", {1,2}},
            {"empty_arr_field", json::array()}
        }},
        {"tags", {"tag1", "tag2", 300}}
    };
    client.set_json(clear_key, object_doc);
    std::cout << "\nInitial object document: " << client.get_json(clear_key).dump(2) << std::endl;
    cleared_count = client.json_clear(clear_key, "$");
    // Expected count:
    // count: 100 -> 0 (1)
    // nested_obj.value: 200 -> 0 (1)
    // nested_obj.sub_arr: [1,2] -> [] (1)
    // nested_obj.empty_arr_field: [] -> [] (0, as it was already empty)
    // tags: ["tag1", "tag2", 300] -> [] (1)
    // Total expected: 1+1+1+1 = 4
    std::cout << "SUCCESS: Cleared root object. Count: " << cleared_count << std::endl;
    std::cout << "Document after clearing root object:\n" << client.get_json(clear_key).dump(2) << std::endl;
    json expected_cleared_obj = {
        {"name", "test"}, {"count", 0}, {"active", true}, {"misc_null", nullptr},
        {"nested_obj", {{"value", 0}, {"sub_arr", json::array()}, {"empty_arr_field", json::array()}}},
        {"tags", json::array()}
    };
    if (client.get_json(clear_key) != expected_cleared_obj || cleared_count != 4) {
        std::cerr << "VERIFICATION ERROR: Clearing root object. Expected count 4. Got: " << cleared_count << std::endl;
    }


    // 3. Clear a nested array (using the state from above)
    // Current state: {"name":"test", "count":0, "active":true, "misc_null":null, "nested_obj":{"value":0, "sub_arr":[], "empty_arr_field":[]}, "tags":[]}
    // Let's reset nested_obj.sub_arr to have items again for this sub-test
    client.set_path(clear_key, "nested_obj.sub_arr", {5,6,7});
    client.set_path(clear_key, "tags", {"new_tag"}); // also reset tags array
    std::cout << "\nDocument for nested clear: " << client.get_json(clear_key).dump(2) << std::endl;

    cleared_count = client.json_clear(clear_key, "nested_obj.sub_arr");
    std::cout << "SUCCESS: Cleared path 'nested_obj.sub_arr'. Count: " << cleared_count << std::endl; // Expected: 1
    std::cout << "Document after clearing 'nested_obj.sub_arr':\n" << client.get_json(clear_key).dump(2) << std::endl;
    if (client.get_json(clear_key)["nested_obj"]["sub_arr"] != json::array() || cleared_count != 1) {
         std::cerr << "VERIFICATION ERROR: Clearing nested_obj.sub_arr. Expected count 1. Got: " << cleared_count << std::endl;
    }

    // 4. Clear a nested object
    // Current state of nested_obj: {"value":0, "sub_arr":[], "empty_arr_field":[]}
    // client.set_path(clear_key, "nested_obj.value", 55); // make a number non-zero for testing
    // No, the 'value' field is already 0 from the root clear. Let's use a fresh object.
    client.del_json(clear_key);
    json fresh_object_doc = {
        {"id", "obj1"},
        {"data", {
            {"num1", 10},
            {"str1", "hello"},
            {"arr1", {1,2}}
        }}
    };
    client.set_json(clear_key, fresh_object_doc);
    std::cout << "\nFresh document for nested object clear: " << client.get_json(clear_key).dump(2) << std::endl;
    cleared_count = client.json_clear(clear_key, "data");
    // Expected count for clearing "data": 1 (for num1) + 1 (for arr1) = 2
    std::cout << "SUCCESS: Cleared path 'data'. Count: " << cleared_count << std::endl;
    std::cout << "Document after clearing 'data':\n" << client.get_json(clear_key).dump(2) << std::endl;
    json expected_data_cleared = {{"id", "obj1"}, {"data", {{"num1",0}, {"str1","hello"}, {"arr1", json::array()}}}};
    if (client.get_json(clear_key) != expected_data_cleared || cleared_count != 2) {
         std::cerr << "VERIFICATION ERROR: Clearing 'data' object. Expected count 2. Got: " << cleared_count << std::endl;
    }
    client.del_json(clear_key);


    // 5. Path to scalar
    client.set_json(clear_key, {{"scalar_num", 123}, {"scalar_str", "hello"}});
    std::cout << "\nInitial document for scalar clear: " << client.get_json(clear_key).dump() << std::endl;
    cleared_count = client.json_clear(clear_key, "scalar_num");
    std::cout << "SUCCESS: 'Cleared' path 'scalar_num'. Count: " << cleared_count << std::endl; // Expected: 1
    std::cout << "Document after 'clearing' scalar_num: " << client.get_json(clear_key).dump() << std::endl; // Should be unchanged
    if (client.get_json(clear_key)["scalar_num"] != 123 || cleared_count != 1) {
        std::cerr << "VERIFICATION ERROR: Clearing path to scalar_num. Expected val 123, count 1. Got val "
                  << client.get_json(clear_key)["scalar_num"].dump() << ", count " << cleared_count << std::endl;
    }
    cleared_count = client.json_clear(clear_key, "scalar_str");
    std::cout << "SUCCESS: 'Cleared' path 'scalar_str'. Count: " << cleared_count << std::endl; // Expected: 1
    std::cout << "Document after 'clearing' scalar_str: " << client.get_json(clear_key).dump() << std::endl; // Should be unchanged
     if (client.get_json(clear_key)["scalar_str"] != "hello" || cleared_count != 1) {
        std::cerr << "VERIFICATION ERROR: Clearing path to scalar_str. Expected val 'hello', count 1. Got val "
                  << client.get_json(clear_key)["scalar_str"].dump() << ", count " << cleared_count << std::endl;
    }
    client.del_json(clear_key);

    // 6. Path does not exist
    client.set_json(clear_key, {{"a", 1}});
    std::cout << "\nInitial document for non-existent path: " << client.get_json(clear_key).dump() << std::endl;
    cleared_count = client.json_clear(clear_key, "non.existent.path");
    std::cout << "SUCCESS: Attempted clear on 'non.existent.path'. Count: " << cleared_count << std::endl; // Expected: 0
    if (cleared_count != 0) {
        std::cerr << "VERIFICATION ERROR: Clearing non-existent path. Expected count 0. Got: " << cleared_count << std::endl;
    }
    client.del_json(clear_key);

    // 7. Key does not exist
    std::string non_existent_key = "sample:clear:no_such_key";
    try { client.del_json(non_existent_key); } catch(...) {} // Ensure key is deleted

    std::cout << "\nAttempting clear on non-existent key '" << non_existent_key << "' with root path:" << std::endl;
    cleared_count = client.json_clear(non_existent_key, "$");
    std::cout << "SUCCESS: Cleared non-existent key with root path. Count: " << cleared_count << std::endl; // Expected: 0
    if (cleared_count != 0) {
         std::cerr << "VERIFICATION ERROR: Clearing non-existent key (root path). Expected count 0. Got: " << cleared_count << std::endl;
    }

    std::cout << "\nAttempting clear on non-existent key '" << non_existent_key << "' with non-root path:" << std::endl;
    try {
        cleared_count = client.json_clear(non_existent_key, "some.path");
        // This should throw PathNotFoundException due to Lua script returning "ERR document not found"
        std::cerr << "ERROR: json_clear on non-existent key with non-root path did not throw PathNotFoundException. Returned count: " << cleared_count << std::endl;
    } catch (const redisjson::PathNotFoundException& e) {
        std::cout << "SUCCESS: Caught expected PathNotFoundException for non-existent key and non-root path: " << e.what() << std::endl;
    } catch (const redisjson::RedisJSONException& e) { // Catch other specific RedisJSON errors
        std::cerr << "ERROR: Unexpected RedisJSONException: " << e.what() << std::endl;
    } catch (const std::exception& e) { // Catch any other std exceptions
        std::cerr << "ERROR: Unexpected std::exception: " << e.what() << std::endl;
    }

    // 8. Clear empty array/object fields
    client.set_json(clear_key, {{"empty_arr", json::array()}, {"empty_obj", json::object()}});
    std::cout << "\nInitial doc with empty containers: " << client.get_json(clear_key).dump() << std::endl;
    cleared_count = client.json_clear(clear_key, "empty_arr");
    std::cout << "SUCCESS: Cleared 'empty_arr'. Count: " << cleared_count << std::endl; // Expected: 0 (was already empty)
    if (cleared_count != 0) {
        std::cerr << "VERIFICATION ERROR: Clearing empty_arr. Expected count 0. Got: " << cleared_count << std::endl;
    }

    cleared_count = client.json_clear(clear_key, "empty_obj");
    std::cout << "SUCCESS: Cleared 'empty_obj'. Count: " << cleared_count << std::endl; // Expected: 0 (was already empty and no numbers to set to 0)
    if (cleared_count != 0) {
        std::cerr << "VERIFICATION ERROR: Clearing empty_obj. Expected count 0. Got: " << cleared_count << std::endl;
    }
    client.del_json(clear_key);
}

void run_arrindex_operations(redisjson::RedisJSONClient& client) {
    print_header("Array Index Operations (JSON.ARRINDEX)");
    std::string key = "sample:arrindex:data";

    // Preparatory cleanup
    try { client.del_json(key); } catch(...) {}

    json doc = {
        {"items", {"apple", "banana", 123, true, "cherry", "banana", nullptr, 45.6}}
    };
    client.set_json(key, doc);
    std::cout << "Initial document: " << client.get_json(key).dump() << std::endl;

    long long index_result;

    // 1. Find string "banana"
    index_result = client.arrindex(key, "items", json("banana"));
    std::cout << "Index of 'banana': " << index_result << " (Expected: 1)" << std::endl;
    if (index_result != 1) std::cerr << "VERIFICATION FAILED for 'banana'" << std::endl;

    // 2. Find number 123
    index_result = client.arrindex(key, "items", json(123));
    std::cout << "Index of 123: " << index_result << " (Expected: 2)" << std::endl;
    if (index_result != 2) std::cerr << "VERIFICATION FAILED for 123" << std::endl;

    // 3. Find boolean true
    index_result = client.arrindex(key, "items", json(true));
    std::cout << "Index of true: " << index_result << " (Expected: 3)" << std::endl;
    if (index_result != 3) std::cerr << "VERIFICATION FAILED for true" << std::endl;

    // 4. Find null
    index_result = client.arrindex(key, "items", json(nullptr));
    std::cout << "Index of null: " << index_result << " (Expected: 6)" << std::endl;
    if (index_result != 6) std::cerr << "VERIFICATION FAILED for null" << std::endl;

    // 5. Find string "banana" starting from index 2
    index_result = client.arrindex(key, "items", json("banana"), 2);
    std::cout << "Index of 'banana' from index 2: " << index_result << " (Expected: 5)" << std::endl;
    if (index_result != 5) std::cerr << "VERIFICATION FAILED for 'banana' from index 2" << std::endl;

    // 6. Find string "banana" in slice [0, 3] (exclusive end for typical slice, but RedisJSON is inclusive for end)
    // Lua script handles end_index as inclusive.
    index_result = client.arrindex(key, "items", json("banana"), 0, 3);
    std::cout << "Index of 'banana' in slice [0, 3]: " << index_result << " (Expected: 1)" << std::endl;
    if (index_result != 1) std::cerr << "VERIFICATION FAILED for 'banana' in slice [0,3]" << std::endl;

    // 7. Find string "banana" in slice [2, 4]
    index_result = client.arrindex(key, "items", json("banana"), 2, 4);
    std::cout << "Index of 'banana' in slice [2, 4]: " << index_result << " (Expected: -1, as 'banana' at 5 is outside)" << std::endl;
    if (index_result != -1) std::cerr << "VERIFICATION FAILED for 'banana' in slice [2,4]" << std::endl;


    // 8. Value not found
    index_result = client.arrindex(key, "items", json("grape"));
    std::cout << "Index of 'grape': " << index_result << " (Expected: -1)" << std::endl;
    if (index_result != -1) std::cerr << "VERIFICATION FAILED for 'grape'" << std::endl;

    // 9. Using negative start index: find "banana" from 3rd last element onwards (-3)
    // Array: {"apple", "banana", 123, true, "cherry", "banana", nullptr, 45.6} (len 8)
    // -1 is 45.6 (idx 7)
    // -2 is null (idx 6)
    // -3 is "banana" (idx 5)
    index_result = client.arrindex(key, "items", json("banana"), -3);
    std::cout << "Index of 'banana' from -3 (3rd last): " << index_result << " (Expected: 5)" << std::endl;
    if (index_result != 5) std::cerr << "VERIFICATION FAILED for 'banana' from -3" << std::endl;

    // 10. Using negative start and end: find "banana" from 4th last (-4) to 2nd last (-2)
    // -4 is "cherry" (idx 4)
    // -2 is null (idx 6)
    // Slice is ["cherry", "banana", nullptr]
    index_result = client.arrindex(key, "items", json("banana"), -4, -2);
    std::cout << "Index of 'banana' in slice [-4, -2]: " << index_result << " (Expected: 5)" << std::endl;
    if (index_result != 5) std::cerr << "VERIFICATION FAILED for 'banana' in slice [-4,-2]" << std::endl;

    // 11. Start index out of bounds (positive)
    index_result = client.arrindex(key, "items", json("apple"), 100);
    std::cout << "Index of 'apple' with start_index 100: " << index_result << " (Expected: -1)" << std::endl;
    if (index_result != -1) std::cerr << "VERIFICATION FAILED for start_index 100" << std::endl;

    // 12. End index out of bounds (negative, before start)
    index_result = client.arrindex(key, "items", json("apple"), 0, -100);
    std::cout << "Index of 'apple' with end_index -100: " << index_result << " (Expected: -1)" << std::endl;
    if (index_result != -1) std::cerr << "VERIFICATION FAILED for end_index -100" << std::endl;

    // 13. Empty array
    client.set_json(key, {{"items", json::array()}});
    std::cout << "Document for empty array test: " << client.get_json(key).dump() << std::endl;
    index_result = client.arrindex(key, "items", json("anything"));
    std::cout << "Index of 'anything' in empty array: " << index_result << " (Expected: -1)" << std::endl;
    if (index_result != -1) std::cerr << "VERIFICATION FAILED for empty array" << std::endl;

    // Error cases
    // 14. Path not an array
    client.set_json(key, {{"items", "not_an_array"}});
    std::cout << "\nTesting path not an array:" << std::endl;
    try {
        client.arrindex(key, "items", json("value"));
        std::cerr << "ERROR: Did not throw for non-array path." << std::endl;
    } catch (const redisjson::TypeMismatchException& e) { // SWSS mode
        std::cout << "SUCCESS (SWSS): Caught TypeMismatchException: " << e.what() << std::endl;
    } catch (const redisjson::LuaScriptException& e) { // Lua mode
        std::cout << "SUCCESS (Lua): Caught LuaScriptException: " << e.what() << std::endl;
        if (std::string(e.what()).find("ERR_NOT_ARRAY") == std::string::npos)
            std::cerr << "VERIFICATION FAILED: Lua error message incorrect for non-array." << std::endl;
    }

    // 15. Path does not exist
    client.set_json(key, {{"other_data", 1}});
    std::cout << "\nTesting path does not exist:" << std::endl;
    try {
        client.arrindex(key, "nonexistent.items", json("value"));
        std::cerr << "ERROR: Did not throw for non-existent path." << std::endl;
    } catch (const redisjson::PathNotFoundException& e) { // SWSS mode
        std::cout << "SUCCESS (SWSS): Caught PathNotFoundException: " << e.what() << std::endl;
    } catch (const redisjson::LuaScriptException& e) { // Lua mode
        std::cout << "SUCCESS (Lua): Caught LuaScriptException: " << e.what() << std::endl;
         if (std::string(e.what()).find("ERR_NOPATH") == std::string::npos && std::string(e.what()).find("ERR_PATH") == std::string::npos)
            std::cerr << "VERIFICATION FAILED: Lua error message incorrect for non-existent path." << std::endl;
    }


    // 16. Key does not exist
    std::string non_existent_key = "sample:arrindex:no_such_key";
    try { client.del_json(non_existent_key); } catch(...) {}
    std::cout << "\nTesting key does not exist:" << std::endl;
    try {
        client.arrindex(non_existent_key, "$", json("value")); // Path $ on non-existent key
        std::cerr << "ERROR: Did not throw for non-existent key." << std::endl;
    } catch (const redisjson::PathNotFoundException& e) { // SWSS mode (get_json throws this)
        std::cout << "SUCCESS (SWSS): Caught PathNotFoundException for non-existent key: " << e.what() << std::endl;
    } catch (const redisjson::LuaScriptException& e) { // Lua mode
        std::cout << "SUCCESS (Lua): Caught LuaScriptException for non-existent key: " << e.what() << std::endl;
        if (std::string(e.what()).find("ERR_NOKEY") == std::string::npos)
            std::cerr << "VERIFICATION FAILED: Lua error message incorrect for non-existent key." << std::endl;
    }


    // Cleanup
    try { client.del_json(key); } catch(...) {}
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
        run_arrinsert_operations(legacy_client);
        run_array_operations_extended(legacy_client);
        run_atomic_operations(legacy_client);
        run_sparse_merge_operations(legacy_client);
        run_object_operations(legacy_client);
        run_numeric_operations(legacy_client);
        run_jsonclear_operations(legacy_client); // Added this line
        run_arrindex_operations(legacy_client); // Added for JSON.ARRINDEX

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
