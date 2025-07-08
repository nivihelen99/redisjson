#include "gtest/gtest.h"
#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h"
#include "redisjson++/exceptions.h"
#include <nlohmann/json.hpp>
#include <algorithm> // For std::sort

using namespace redisjson;
using json = nlohmann::json;

class JsonObjectKeysLuaTest : public ::testing::Test {
protected:
    ClientConfig client_config_;
    RedisConnectionManager conn_manager_;
    LuaScriptManager script_manager_;
    bool live_redis_available_ = false;
    std::string test_key_ = "json_objkeys_test_key";

    JsonObjectKeysLuaTest() :
        client_config_(), // Default config (localhost:6379)
        conn_manager_(client_config_),
        script_manager_(&conn_manager_) {
        try {
            redisjson::RedisConnection test_conn(
                client_config_.host, client_config_.port, client_config_.password,
                client_config_.database, client_config_.timeout);
            if (test_conn.connect() && test_conn.ping()) {
                live_redis_available_ = true;
            }
        } catch (const std::exception& e) {
            live_redis_available_ = false;
            std::cerr << "Live Redis instance not detected for JsonObjectKeysLuaTest: " << e.what() << std::endl;
        }

        if (live_redis_available_) {
            try {
                script_manager_.preload_builtin_scripts(); // Ensure "json_object_keys" is loaded
                 // Clean up any existing test key before each test run
                auto conn = conn_manager_.get_connection();
                conn->command("DEL %s", test_key_.c_str());
            } catch (const std::exception& e) {
                std::cerr << "Error preloading scripts or cleaning key in test setup: " << e.what() << std::endl;
                // Depending on severity, might set live_redis_available_ to false
            }
        } else {
            std::cout << "Skipping JsonObjectKeysLuaTest tests that require live Redis." << std::endl;
        }
    }

    void SetUp() override {
        if (live_redis_available_) {
            // Clean up the test key before each test
            try {
                auto conn = conn_manager_.get_connection();
                conn->command("DEL %s", test_key_.c_str());
            } catch (const std::exception& e) {
                std::cerr << "Failed to delete test key in SetUp: " << e.what() << std::endl;
                // This could affect test isolation.
            }
        }
    }

    void TearDown() override {
        if (live_redis_available_) {
            // Clean up the test key after each test
            try {
                auto conn = conn_manager_.get_connection();
                conn->command("DEL %s", test_key_.c_str());
            } catch (const std::exception& e) {
                 // std::cerr << "Failed to delete test key in TearDown: " << e.what() << std::endl;
            }
        }
    }

    // Helper to set JSON value in Redis
    void set_json(const std::string& key, const json& value) {
        auto conn = conn_manager_.get_connection();
        std::string value_str = value.dump();
        redisReply* reply = conn->command("SET %s %s", key.c_str(), value_str.c_str());
        ASSERT_NE(reply, nullptr);
        ASSERT_STREQ(reply->str, "OK");
        freeReplyObject(reply);
    }

    // Helper to execute the script and return result as json
    // Handles cases where script might return nil (parsed as json(nullptr))
    // or a JSON array string.
    json execute_objkeys_script(const std::string& key, const std::string& path) {
        return script_manager_.execute_script("json_object_keys", {key}, {path});
    }
};

TEST_F(JsonObjectKeysLuaTest, EmptyObjectAtRoot) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    set_json(test_key_, json::object());
    json result = execute_objkeys_script(test_key_, "$");
    ASSERT_TRUE(result.is_array());
    EXPECT_TRUE(result.empty());
}

TEST_F(JsonObjectKeysLuaTest, SimpleNonEmptyObjectAtRoot) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"key1", "value1"}, {"key2", 123}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$");
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys[0], "key1");
    EXPECT_EQ(keys[1], "key2");
}

TEST_F(JsonObjectKeysLuaTest, NestedEmptyObject) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"nested", json::object()}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.nested");
    ASSERT_TRUE(result.is_array());
    EXPECT_TRUE(result.empty());
}

TEST_F(JsonObjectKeysLuaTest, NestedNonEmptyObject) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"parent", {{"child1", true}, {"child2", "hello"}}}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.parent");
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys[0], "child1");
    EXPECT_EQ(keys[1], "child2");
}

TEST_F(JsonObjectKeysLuaTest, PathToNonExistentObject) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"exists", 123}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.nonexistent");
    // The script returns Redis nil if path doesn't lead to an object,
    // which redis_reply_to_json parses as json(nullptr).
    EXPECT_TRUE(result.is_null());
}

TEST_F(JsonObjectKeysLuaTest, PathToNonObjectTypeArray) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"arr", {1, 2, 3}}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.arr");
    EXPECT_TRUE(result.is_null()); // OBJKEYS on array should return nil (or error, script returns nil)
}

TEST_F(JsonObjectKeysLuaTest, PathToNonObjectTypeString) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"str", "i am a string"}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.str");
    EXPECT_TRUE(result.is_null()); // OBJKEYS on string should return nil
}

TEST_F(JsonObjectKeysLuaTest, PathToNonObjectTypeNumber) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"num", 123.45}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.num");
    EXPECT_TRUE(result.is_null()); // OBJKEYS on number should return nil
}


TEST_F(JsonObjectKeysLuaTest, PathToNonObjectTypeBoolean) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"bool", true}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.bool");
    EXPECT_TRUE(result.is_null());
}

TEST_F(JsonObjectKeysLuaTest, PathToNonObjectTypeNull) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"null_val", nullptr}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$.null_val");
    EXPECT_TRUE(result.is_null());
}

TEST_F(JsonObjectKeysLuaTest, NonExistentRedisKey) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    // Key "nonexistent_key" is guaranteed not to exist by SetUp/TearDown logic for test_key_
    // and we use a different key here.
    json result = execute_objkeys_script("nonexistent_key_for_objkeys", "$");
    EXPECT_TRUE(result.is_null()); // Script returns nil if key doesn't exist
}

TEST_F(JsonObjectKeysLuaTest, EmptyPathArgumentSameAsRoot) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"keyA", "valA"}, {"keyB", "valB"}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, ""); // Empty path
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys[0], "keyA");
    EXPECT_EQ(keys[1], "keyB");
}

TEST_F(JsonObjectKeysLuaTest, RootPathArgument) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {{"keyA", "valA"}, {"keyB", "valB"}};
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$"); // Explicit root path
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 2);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys[0], "keyA");
    EXPECT_EQ(keys[1], "keyB");
}

TEST_F(JsonObjectKeysLuaTest, MalformedPath) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    set_json(test_key_, {{"a",1}});
    // The script should return an error reply for malformed paths, which LuaScriptManager::execute_script
    // should convert into a LuaScriptException.
    // Example: parse_path returns an error table.
    EXPECT_THROW(execute_objkeys_script(test_key_, "$.[unclosed_bracket"), LuaScriptException);
    EXPECT_THROW(execute_objkeys_script(test_key_, "$.obj..field"), LuaScriptException); // Double dot
}

TEST_F(JsonObjectKeysLuaTest, PathToEmptyObjectFieldFixVerification) {
    // This test specifically verifies the fix for returning `[]` instead of `{}` for empty objects.
    // The Lua script's logic: `if #keys_array == 0 then return "[]" else return cjson.encode(keys_array) end`
    // should ensure this.
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    json data = {
        {"top_level_empty", json::object()},
        {"nested_empty", {{"child_empty", json::object()}}}
    };
    set_json(test_key_, data);

    // Test $.top_level_empty
    json result_top = execute_objkeys_script(test_key_, "$.top_level_empty");
    ASSERT_TRUE(result_top.is_array()) << "Expected JSON array, got: " << result_top.dump();
    EXPECT_TRUE(result_top.empty()) << "Expected empty array, got: " << result_top.dump();

    // Test $.nested_empty.child_empty
    json result_nested = execute_objkeys_script(test_key_, "$.nested_empty.child_empty");
    ASSERT_TRUE(result_nested.is_array()) << "Expected JSON array, got: " << result_nested.dump();
    EXPECT_TRUE(result_nested.empty()) << "Expected empty array, got: " << result_nested.dump();
}

TEST_F(JsonObjectKeysLuaTest, ObjectWithNumericKeys) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    // JSON objects can have string keys that look like numbers.
    // Lua tables might represent these differently, but cjson should handle it.
    // The script converts all keys to strings using tostring(k).
    json data = {{"1", "one"}, {"2", "two"}, {"obj", {{"10", "ten"}}}};
    set_json(test_key_, data);

    json result_root = execute_objkeys_script(test_key_, "$");
    ASSERT_TRUE(result_root.is_array());
    ASSERT_EQ(result_root.size(), 3);
    std::vector<std::string> keys_root = result_root.get<std::vector<std::string>>();
    std::sort(keys_root.begin(), keys_root.end());
    EXPECT_EQ(keys_root[0], "1");
    EXPECT_EQ(keys_root[1], "2");
    EXPECT_EQ(keys_root[2], "obj");

    json result_nested = execute_objkeys_script(test_key_, "$.obj");
    ASSERT_TRUE(result_nested.is_array());
    ASSERT_EQ(result_nested.size(), 1);
    std::vector<std::string> keys_nested = result_nested.get<std::vector<std::string>>();
    EXPECT_EQ(keys_nested[0], "10");
}

TEST_F(JsonObjectKeysLuaTest, ObjectWithComplexKeys) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    // Keys with spaces, special characters (if path parser and Lua script handle them)
    // The current simple path parser in Lua helpers might struggle with complex keys in path segments.
    // However, OBJKEYS itself operates on the object found at path, so keys *within* that object are fine.
    json data = {
        {"simple", "v"},
        {"key with space", "v space"},
        {"key.with.dot", "v dot"},
        {"key[with]bracket", "v bracket"}
    };
    set_json(test_key_, data);
    json result = execute_objkeys_script(test_key_, "$");
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 4);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    std::sort(keys.begin(), keys.end());

    // Expected keys, sorted:
    std::vector<std::string> expected_keys = {"key with space", "key.with.dot", "key[with]bracket", "simple"};
    std::sort(expected_keys.begin(), expected_keys.end());

    EXPECT_EQ(keys, expected_keys);
}

TEST_F(JsonObjectKeysLuaTest, PathToArrayOfStringKeys) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    // This is an edge case for the path itself, not OBJKEYS behavior primarily.
    // Our path parser `parse_path` expects dot notation for objects and `[index]` for arrays.
    // If a path like `$.obj.arr_key[0]` points to an object, OBJKEYS should work.
    json data = {
        {"obj", {
            {"arr_key", json::array({ // arr_key is an array
                json::object({{"id", "obj_in_arr1"}}), // First element is an object
                json::object({{"id", "obj_in_arr2"}})  // Second element is an object
            })}
        }}
    };
    set_json(test_key_, data);

    // Get keys of the first object in the array $.obj.arr_key[0]
    json result = execute_objkeys_script(test_key_, "$.obj.arr_key[0]");
    ASSERT_TRUE(result.is_array());
    ASSERT_EQ(result.size(), 1);
    std::vector<std::string> keys = result.get<std::vector<std::string>>();
    EXPECT_EQ(keys[0], "id");

    // Get keys of the second object in the array $.obj.arr_key[1]
    json result2 = execute_objkeys_script(test_key_, "$.obj.arr_key[1]");
    ASSERT_TRUE(result2.is_array());
    ASSERT_EQ(result2.size(), 1);
    std::vector<std::string> keys2 = result2.get<std::vector<std::string>>();
    EXPECT_EQ(keys2[0], "id");

    // Path to an out-of-bounds array index that would contain an object
    json result_oob = execute_objkeys_script(test_key_, "$.obj.arr_key[5]");
    EXPECT_TRUE(result_oob.is_null()); // Path doesn't lead to an object
}

TEST_F(JsonObjectKeysLuaTest, KeyNotAJsonDocument) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";
    auto conn = conn_manager_.get_connection();
    redisReply* reply = conn->command("SET %s %s", test_key_.c_str(), "this is not json");
    ASSERT_NE(reply, nullptr);
    ASSERT_STREQ(reply->str, "OK");
    freeReplyObject(reply);

    // The script attempts cjson.decode, which will fail.
    // It returns an error reply: redis.error_reply('ERR_DECODE ...')
    // This should be caught and thrown as LuaScriptException by LuaScriptManager.
    EXPECT_THROW(execute_objkeys_script(test_key_, "$"), LuaScriptException);
}

// Note: The Lua script uses a basic path parser. More complex paths with escaped characters
// or quoted keys in the path string itself might not be supported by this parser.
// The tests above focus on the OBJKEYS logic given a successfully resolved path.

// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }
