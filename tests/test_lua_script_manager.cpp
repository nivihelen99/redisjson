#include "gtest/gtest.h"
#include <gmock/gmock.h> // For EXPECT_THAT and HasSubstr
#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h" // Required by LuaScriptManager
#include "redisjson++/exceptions.h"
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr

using namespace redisjson;
using json = nlohmann::json;

// These tests require a running Redis instance that has cjson available for Lua.
// Or a sophisticated mocking framework for RedisConnection and hiredis replies.

class LuaScriptManagerTest : public ::testing::Test {
protected:
    // Minimal ClientConfig for ConnectionManager
    // For these tests, we assume Redis is running at default location.
    // Tests might be skipped or fail if Redis is not available.
    ClientConfig client_config_;
    RedisConnectionManager conn_manager_;
    LuaScriptManager script_manager_;

    // Flag to enable/disable tests requiring live Redis.
    // In a real CI, this could be controlled by environment variables or build tags.
    bool live_redis_available_ = false;

    LuaScriptManagerTest() :
        client_config_(), // Default config (localhost:6379)
        conn_manager_(client_config_),
        script_manager_(&conn_manager_) {

        // Basic check if Redis might be running for more meaningful tests
        // This is a very crude check. A proper setup would use a dedicated test Redis.
        try {
        // Directly use RedisConnection for the availability check
        // Using default client_config_ for host, port, timeout settings.
        redisjson::RedisConnection test_conn(
            client_config_.host,
            client_config_.port,
            client_config_.password,
            client_config_.database,
            client_config_.timeout
        );
        if (test_conn.connect() && test_conn.ping()) {
                live_redis_available_ = true;
            }
        // test_conn will disconnect and free context in its destructor
        } catch (const std::exception& e) {
            // Connection failed, assume Redis not available for tests
            live_redis_available_ = false;
        std::cerr << "Live Redis instance not detected for LuaScriptManager tests (using direct connection): " << e.what() << std::endl;
        }
        if (!live_redis_available_) {
             std::cout << "Skipping LuaScriptManager tests that require live Redis." << std::endl;
        }
    }

    void SetUp() override {
        if (live_redis_available_) {
            // Clear any existing scripts from a previous run, and local cache
            try {
                script_manager_.clear_all_scripts_cache();
            } catch (const std::exception& e) {
                // Log and potentially fail if cleanup is critical
                std::cerr << "Warning: Could not clear Redis script cache: " << e.what() << std::endl;
            }
        }
        script_manager_.clear_local_script_cache(); // Always clear local
    }
};

TEST_F(LuaScriptManagerTest, Construction) {
    ASSERT_NO_THROW({
        LuaScriptManager sm(&conn_manager_);
    });
}

TEST_F(LuaScriptManagerTest, LoadScript) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    const std::string script_name = "test_echo";
    const std::string script_body = "return ARGV[1]";

    ASSERT_NO_THROW(script_manager_.load_script(script_name, script_body));
    EXPECT_TRUE(script_manager_.is_script_loaded(script_name));
}

TEST_F(LuaScriptManagerTest, ExecuteLoadedScript) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    const std::string script_name = "test_echo_exec";
    const std::string script_body = "return ARGV[1]"; // Script returns the first arg
    script_manager_.load_script(script_name, script_body);

    std::vector<std::string> keys = {};
    std::vector<std::string> args = {"Hello Lua!"};

    json result;
    ASSERT_NO_THROW({
        result = script_manager_.execute_script(script_name, keys, args);
    });
    // The script `return ARGV[1]` returns a plain string.
    // redis_reply_to_json will try to parse this as a JSON string.
    // If Lua returns "Hello Lua!", redis_reply_to_json will try json::parse("Hello Lua!"). This fails.
    // The Lua script needs to return a valid JSON encoded string: return cjson.encode(ARGV[1])
    // For now, let's adjust the test or assume script returns JSON-parseable string.
    // Let's modify script to return JSON string:
    const std::string script_name_json = "test_echo_json_exec";
    const std::string script_body_json = "return cjson.encode(ARGV[1])";
    ASSERT_NO_THROW(script_manager_.load_script(script_name_json, script_body_json));

    json result_json;
    ASSERT_NO_THROW({
        result_json = script_manager_.execute_script(script_name_json, keys, args);
    });
    EXPECT_TRUE(result_json.is_string());
    EXPECT_EQ(result_json.get<std::string>(), "Hello Lua!");
}

// --- Tests for JSON_ARRAY_INSERT_LUA ---
class LuaScriptManagerArrInsertTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:arrinsert";

    void SetUp() override {
        LuaScriptManagerTest::SetUp();
        if (live_redis_available_) {
            try {
                script_manager_.preload_builtin_scripts();
                ASSERT_TRUE(script_manager_.is_script_loaded("json_array_insert"));
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);
            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping ArrInsert tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping ArrInsert tests, live Redis required.";
        }
    }

    void TearDown() override {
        if (live_redis_available_) {
            try {
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);
            } catch (...) { /* ignore cleanup errors */ }
        }
        LuaScriptManagerTest::TearDown();
    }

    void set_initial_json(const json& doc) {
        auto conn = conn_manager_.get_connection();
        std::string doc_str = doc.dump();
        redisReply* reply = conn->command("SET %s %s", test_key_.c_str(), doc_str.c_str());
        ASSERT_NE(reply, nullptr);
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS) << "SET command failed: " << (reply->str ? reply->str : "Unknown error");
        ASSERT_STREQ(reply->str, "OK");
        freeReplyObject(reply);
    }

    json get_current_json() {
        auto conn = conn_manager_.get_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("GET %s", test_key_.c_str())));
        if (!reply || reply->type != REDIS_REPLY_STRING) {
            return json(nullptr);
        }
        return json::parse(reply->str);
    }
};

TEST_F(LuaScriptManagerArrInsertTest, InsertSingleValueMiddle) {
    set_initial_json(json::array({"a", "c"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "1", R"("b")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertMultipleValuesMiddle) {
    set_initial_json(json::array({"a", "d"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "1", R"("b")", R"("c")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 4);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c", "d"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertAtBeginningIndexZero) {
    set_initial_json(json::array({"b", "c"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "0", R"("a")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertAtEndPositiveIndexLarge) {
    set_initial_json(json::array({"a", "b"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "100", R"("c")"}); // Index 100 -> effective end
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertAtEndSpecificIndex) {
    set_initial_json(json::array({"a", "b"}));
    // Client index 2 means after current last element (0-indexed 'b' at 1), so at Lua 1-based index 3.
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "2", R"("c")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}


TEST_F(LuaScriptManagerArrInsertTest, InsertNegativeIndexBeforeLast) {
    set_initial_json(json::array({"a", "c"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "-1", R"("b")"}); // -1 inserts before 'c'
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertNegativeIndexAtBeginning) {
    set_initial_json(json::array({"b", "c"}));
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "-100", R"("a")"}); // Large negative index -> beginning
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    EXPECT_EQ(get_current_json(), json::array({"a", "b", "c"}));
}


TEST_F(LuaScriptManagerArrInsertTest, InsertIntoEmptyArray) {
    set_initial_json(json::array());
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "0", R"("a")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1);
    EXPECT_EQ(get_current_json(), json::array({"a"}));

    set_initial_json(json::array()); // Reset
    result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "5", R"("b")"}); // Index > 0 on empty
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1);
    EXPECT_EQ(get_current_json(), json::array({"b"}));

    set_initial_json(json::array()); // Reset
    result = script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "-5", R"("c")"}); // Index < 0 on empty
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1);
    EXPECT_EQ(get_current_json(), json::array({"c"}));
}

TEST_F(LuaScriptManagerArrInsertTest, InsertIntoNestedArray) {
    json doc = {{"data", {{"list", {"x", "z"}}}}};
    set_initial_json(doc);
    json result = script_manager_.execute_script("json_array_insert", {test_key_}, {"data.list", "1", R"("y")"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 3);
    json expected_doc = {{"data", {{"list", {"x", "y", "z"}}}}};
    EXPECT_EQ(get_current_json(), expected_doc);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorKeyNotFound) {
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_array_insert", {"nonexistentkey"}, {"$", "0", R"("a")"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_NOKEY"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorPathNotFound) {
    set_initial_json({{"some", "object"}});
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_array_insert", {test_key_}, {"data.list", "0", R"("a")"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_NOPATH"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorNotAnArray) {
    set_initial_json({{"data", "not an array"}});
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_array_insert", {test_key_}, {"data", "0", R"("a")"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_NOT_ARRAY"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorInvalidIndexString) {
    set_initial_json(json::array({"a"}));
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "notanumber", R"("b")"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_INDEX"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorNotEnoughArguments) {
    set_initial_json(json::array({"a"}));
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "0"}); // Missing value
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_ARG_COUNT"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrInsertTest, ErrorValueNotJson) {
    set_initial_json(json::array({"a"}));
    EXPECT_THROW({
        try {
            // Pass a string that is not valid JSON for the value
            script_manager_.execute_script("json_array_insert", {test_key_}, {"$", "0", "this is not json"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_DECODE_ARG"));
            throw;
        }
    }, LuaScriptException);
}


// --- Tests for JSON_OBJECT_LENGTH_LUA ---
class LuaScriptManagerObjLenTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:objlen";

    void SetUp() override {
        LuaScriptManagerTest::SetUp(); // Call base SetUp
        if (live_redis_available_) {
            try {
                // Ensure json_object_length script is loaded (preload_builtin_scripts does this)
                script_manager_.preload_builtin_scripts();
                ASSERT_TRUE(script_manager_.is_script_loaded("json_object_length"));

                // Clean up the test key before each test
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);

            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping ObjLen tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping ObjLen tests, live Redis required.";
        }
    }

    void TearDown() override {
        if (live_redis_available_) {
            try {
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                 if (reply) freeReplyObject(reply);
            } catch (...) { /* ignore cleanup errors */ }
        }
        LuaScriptManagerTest::TearDown();
    }

    // Helper to set initial JSON for testing
    void set_initial_json(const json& doc) {
        auto conn = conn_manager_.get_connection();
        std::string doc_str = doc.dump();
        redisReply* reply = conn->command("SET %s %s", test_key_.c_str(), doc_str.c_str());
        ASSERT_NE(reply, nullptr) << "Failed to SET initial JSON for test. Context: " << (conn->get_context() ? conn->get_context()->errstr : "N/A");
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS) << "SET command failed: " << (reply->str ? reply->str : "Unknown error");
        ASSERT_STREQ(reply->str, "OK");
        freeReplyObject(reply);
    }
};

TEST_F(LuaScriptManagerObjLenTest, LengthOfRootObject) {
    json initial_doc = {{"name", "John"}, {"age", 30}, {"city", "New York"}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_object_length", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<size_t>(), 3);
}

TEST_F(LuaScriptManagerObjLenTest, LengthOfNestedObject) {
    json initial_doc = {
        {"user", {{"name", "Jane"}, {"id", 101}}},
        {"settings", {{"theme", "dark"}, {"notifications", true}}}
    };
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_object_length", {test_key_}, {"settings"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<size_t>(), 2);
}

TEST_F(LuaScriptManagerObjLenTest, LengthOfEmptyObject) {
    json initial_doc = {{"empty_obj", json::object()}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_object_length", {test_key_}, {"empty_obj"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<size_t>(), 0);
}

TEST_F(LuaScriptManagerObjLenTest, PathPointsToArray) {
    json initial_doc = {{"my_array", {1, 2, 3}}};
    set_initial_json(initial_doc);

    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_object_length", {test_key_}, {"my_array"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_TYPE"));
            EXPECT_THAT(e.what(), ::testing::HasSubstr("Path value is an array"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerObjLenTest, PathPointsToScalar) {
    json initial_doc = {{"my_string", "hello"}};
    set_initial_json(initial_doc);

    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_object_length", {test_key_}, {"my_string"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_TYPE"));
            EXPECT_THAT(e.what(), ::testing::HasSubstr("not an object or array")); // Lua script's error message
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerObjLenTest, PathNotFound) {
    json initial_doc = {{"user", {{"name", "Jane"}}}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_object_length", {test_key_}, {"user.nonexistent"});
    EXPECT_TRUE(result.is_null()); // Lua script returns nil for path not found
}

TEST_F(LuaScriptManagerObjLenTest, KeyNotFound) {
    // test_key_ is guaranteed to be clean by SetUp
    json result = script_manager_.execute_script("json_object_length", {test_key_}, {"$"});
    EXPECT_TRUE(result.is_null()); // Lua script returns nil for key not found
}

TEST_F(LuaScriptManagerObjLenTest, MalformedJsonDocument) {
    // Manually set a non-JSON string in Redis
    auto conn = conn_manager_.get_connection();
    redisReply* reply_set = conn->command("SET %s %s", test_key_.c_str(), "this is not json");
    ASSERT_NE(reply_set, nullptr);
    ASSERT_STREQ(reply_set->str, "OK");
    freeReplyObject(reply_set);

    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_object_length", {test_key_}, {"$"});
        } catch (const LuaScriptException& e) {
            // cjson.decode might raise a direct Lua error (e.g., "Expected value but found invalid token")
            // or it might return an error string that our script wraps with "ERR_DECODE".
            // We need to be flexible here.
            EXPECT_THAT(e.what(), ::testing::MatchesRegex(".*(decode JSON|invalid token|ERR_DECODE|lexical error|parse error).*"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerObjLenTest, PathIsInvalid) {
    json initial_doc = {{"value", 10}};
    set_initial_json(initial_doc);
     EXPECT_THROW({
        try {
            script_manager_.execute_script("json_object_length", {test_key_}, {"user..name"}); // Invalid path with double dots
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_PATH Invalid path string"));
            throw;
        }
    }, LuaScriptException);
}


TEST_F(LuaScriptManagerTest, ExecuteNonExistentScriptLocal) {
    // No live Redis needed as it should fail on local cache check
    EXPECT_THROW(script_manager_.execute_script("nonexistent_local", {}, {}), LuaScriptException);
}

TEST_F(LuaScriptManagerTest, ExecuteNonExistentScriptOnServerNOSCRIPT) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    // Load a script locally, but don't ensure it's on server (or flush server)
    const std::string script_name = "test_noscript";
    const std::string script_body = "return 'test'"; // Simple script

    // Manually add to local cache without loading to Redis to simulate SCRIPT FLUSH scenario
    // This is a bit hacky for testing NOSCRIPT.
    // A better way would be to load it, then SCRIPT FLUSH, then try to execute.

    // Simulate loading it (gets an SHA)
    ASSERT_NO_THROW(script_manager_.load_script(script_name, script_body));

    // Now, flush scripts from Redis server to ensure NOSCRIPT error
    try {
        auto conn = conn_manager_.get_connection();
        redisReply* reply = conn->command("SCRIPT FLUSH"); // Assuming direct command execution capability
        if (reply) freeReplyObject(reply);
        else throw ConnectionException("Failed to flush for test setup");
    } catch(const std::exception& e) {
        GTEST_SKIP() << "Skipping NOSCRIPT test, could not flush Redis scripts: " << e.what();
    }

    EXPECT_THROW(script_manager_.execute_script(script_name, {}, {}), LuaScriptException);
}


TEST_F(LuaScriptManagerTest, PreloadBuiltinScripts) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    ASSERT_NO_THROW(script_manager_.preload_builtin_scripts());
    EXPECT_TRUE(script_manager_.is_script_loaded("json_get_set"));
    EXPECT_TRUE(script_manager_.is_script_loaded("json_compare_set"));

    // Try executing one of them (basic placeholder version)
    // json_get_set: KEYS[1]=key, ARGV[1]=path_str, ARGV[2]=new_value_json_str
    // This requires `key` to exist and be a JSON string.
    // Setup:
    std::string test_key = "luatest:doc1";
    std::string initial_json_doc = R"({"field1":"old_value", "field2":10})";
    std::string new_field_value = R"("new_value")"; // JSON string for the new value

    try {
        auto conn = conn_manager_.get_connection();
        redisReply* reply = conn->command("SET %s %s", test_key.c_str(), initial_json_doc.c_str());
        if(!reply || strcmp(reply->str, "OK")!=0) {
             if(reply) freeReplyObject(reply);
             throw std::runtime_error("Failed to SET test key for Lua script");
        }
        freeReplyObject(reply);

        json result = script_manager_.execute_script("json_get_set", {test_key}, {"field1", new_field_value});
        EXPECT_EQ(result.get<std::string>(), "old_value"); // Script returns old value at path

        // Verify change
        reply = conn->command("GET %s", test_key.c_str());
        ASSERT_NE(reply, nullptr);
        ASSERT_EQ(reply->type, REDIS_REPLY_STRING);
        json updated_doc = json::parse(reply->str);
        freeReplyObject(reply);
        EXPECT_EQ(updated_doc["field1"], "new_value");

    } catch (const std::exception& e) {
        FAIL() << "Exception during preload_builtin_scripts test execution: " << e.what();
    }
}

TEST_F(LuaScriptManagerTest, ClearLocalCache) {
    if (live_redis_available_) { // Load something first if Redis is up
      ASSERT_NO_THROW(script_manager_.load_script("temp_script", "return 1"));
      EXPECT_TRUE(script_manager_.is_script_loaded("temp_script"));
    } else { // If no Redis, manually populate local cache for test purpose
        // This is not ideal, but tests the clear_local_script_cache logic itself.
        // LuaScriptManager doesn't have a public way to add to cache without Redis.
        // So, this test path is limited without live Redis.
        // For now, if no live Redis, this test doesn't verify much for local clear after load.
    }
    script_manager_.clear_local_script_cache();
    EXPECT_FALSE(script_manager_.is_script_loaded("temp_script")); // Should be gone from local
}

TEST_F(LuaScriptManagerTest, ClearAllScriptsCache) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    ASSERT_NO_THROW(script_manager_.load_script("another_temp_script", "return 2"));
    EXPECT_TRUE(script_manager_.is_script_loaded("another_temp_script"));

    ASSERT_NO_THROW(script_manager_.clear_all_scripts_cache());
    EXPECT_FALSE(script_manager_.is_script_loaded("another_temp_script"));
    // And it should be flushed from server (tested implicitly if next load works or NOSCRIPT if not reloaded)
}

TEST_F(LuaScriptManagerTest, ConnectionPoolReturnsConnections) {
    if (!live_redis_available_) GTEST_SKIP() << "Skipping test, live Redis required.";

    // Configure a client with a small connection pool
    ClientConfig small_pool_config; // Defaults to localhost:6379
    small_pool_config.connection_pool_size = 2; // Small pool
    RedisConnectionManager small_conn_manager(small_pool_config);
    LuaScriptManager sm(&small_conn_manager);

    const std::string script_name = "test_pool_echo";
    // Script that returns a JSON encoded string to be compatible with execute_script
    const std::string script_body = "return cjson.encode(ARGV[1])"; 

    // Load the script once
    ASSERT_NO_THROW(sm.load_script(script_name, script_body));
    EXPECT_TRUE(sm.is_script_loaded(script_name));

    // Call execute_script multiple times, more than the pool size
    int num_calls = 5;
    for (int i = 0; i < num_calls; ++i) {
        std::vector<std::string> keys = {};
        std::vector<std::string> args = {"Call " + std::to_string(i)};
        json result;
        ASSERT_NO_THROW({
            result = sm.execute_script(script_name, keys, args);
        }) << "Failed during execute_script call number " << i;
        EXPECT_EQ(result.get<std::string>(), "Call " + std::to_string(i));
    }

    // Check connection stats
    ConnectionStats stats = small_conn_manager.get_stats();
    EXPECT_EQ(stats.active_connections, 0) << "Active connections should be 0 after operations.";
    // Depending on exact pool behavior (e.g. creation on demand vs up front)
    // idle_connections could be up to small_pool_config.connection_pool_size
    // If connections are created on demand and returned, total and idle should reflect that.
    // Let's assume the pool creates up to its max size if needed.
    EXPECT_LE(stats.idle_connections, small_pool_config.connection_pool_size); 
    EXPECT_GT(stats.idle_connections, 0) << "Should have at least one idle connection if pool was used.";
    EXPECT_LE(stats.total_connections, small_pool_config.connection_pool_size);


    // Test with clear_all_scripts_cache as well
    ASSERT_NO_THROW(sm.clear_all_scripts_cache());
    ConnectionStats stats_after_flush = small_conn_manager.get_stats();
    EXPECT_EQ(stats_after_flush.active_connections, 0);
    EXPECT_LE(stats_after_flush.idle_connections, small_pool_config.connection_pool_size);

    // Test with load_script multiple times (though it's usually called once per script)
    for (int i = 0; i < num_calls; ++i) {
        ASSERT_NO_THROW(sm.load_script(script_name + std::to_string(i), script_body));
    }
    ConnectionStats stats_after_multiple_loads = small_conn_manager.get_stats();
    EXPECT_EQ(stats_after_multiple_loads.active_connections, 0);
    EXPECT_LE(stats_after_multiple_loads.idle_connections, small_pool_config.connection_pool_size);
}

// --- Tests for JSON_NUMINCRBY_LUA ---
class LuaScriptManagerNumIncrByTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:numincr";

    void SetUp() override {
        LuaScriptManagerTest::SetUp(); // Call base SetUp
        if (live_redis_available_) {
            // Ensure the script is loaded for these tests
            try {
                script_manager_.preload_builtin_scripts(); // This loads all, including json_numincrby
                ASSERT_TRUE(script_manager_.is_script_loaded("json_numincrby"));

                // Clean up the test key before each test
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);

            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping NumIncrBy tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping NumIncrBy tests, live Redis required.";
        }
    }

    void TearDown() override {
        if (live_redis_available_) {
            try {
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                 if (reply) freeReplyObject(reply);
            } catch (...) { /* ignore cleanup errors */ }
        }
        LuaScriptManagerTest::TearDown();
    }

    // Helper to set initial JSON for testing NUMINCRBY
    void set_initial_json(const json& doc) {
        auto conn = conn_manager_.get_connection();
        std::string doc_str = doc.dump();
        redisReply* reply = conn->command("SET %s %s", test_key_.c_str(), doc_str.c_str());
        ASSERT_NE(reply, nullptr) << "Failed to SET initial JSON for test. Context: " << (conn->get_context() ? conn->get_context()->errstr: "N/A");
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS) << "SET command failed: " << (reply->str ? reply->str : "Unknown error");
        ASSERT_STREQ(reply->str, "OK");
        freeReplyObject(reply);
    }
};

TEST_F(LuaScriptManagerNumIncrByTest, IncrementExistingInteger) {
    json initial_doc = {{"value", 10}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "5"});
    ASSERT_TRUE(result.is_number());
    EXPECT_EQ(result.get<double>(), 15.0);

    // Verify directly from Redis
    auto conn = conn_manager_.get_connection();
    RedisReplyPtr reply(static_cast<redisReply*>(conn->command("GET %s", test_key_.c_str())));
    json updated_doc = json::parse(reply->str);
    EXPECT_EQ(updated_doc["value"], 15);
}

TEST_F(LuaScriptManagerNumIncrByTest, IncrementExistingFloat) {
    json initial_doc = {{"value", 10.5}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "0.25"});
    ASSERT_TRUE(result.is_number());
    EXPECT_DOUBLE_EQ(result.get<double>(), 10.75);
}

TEST_F(LuaScriptManagerNumIncrByTest, DecrementExistingNumber) {
    json initial_doc = {{"value", 20}};
    set_initial_json(initial_doc);

    json result = script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "-5"});
    ASSERT_TRUE(result.is_number());
    EXPECT_EQ(result.get<double>(), 15.0);
}

TEST_F(LuaScriptManagerNumIncrByTest, KeyDoesNotExist) {
    // Key test_key_ is guaranteed to be clean by SetUp/TearDown
    try {
         script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "5"});
         FAIL() << "Expected LuaScriptException for non-existent key";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_NOKEY"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, PathDoesNotExist) {
    json initial_doc = {{"other_value", 10}};
    set_initial_json(initial_doc);
    try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "5"});
        FAIL() << "Expected LuaScriptException for non-existent path";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_NOPATH"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, ValueAtNotNumber) {
    json initial_doc = {{"value", "not a number"}};
    set_initial_json(initial_doc);
     try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "5"});
        FAIL() << "Expected LuaScriptException for non-numeric value at path";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_TYPE"));
        EXPECT_THAT(e.what(), ::testing::HasSubstr("is not a number"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, IncrementValueNotValidNumber) {
    json initial_doc = {{"value", 10}};
    set_initial_json(initial_doc);
    try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "abc"});
        FAIL() << "Expected LuaScriptException for invalid increment value";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_ARG_CONVERT"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, PathIsRoot) {
    json initial_doc = {{"value", 10}}; // content doesn't matter as it should fail on path
    set_initial_json(initial_doc);
    try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"$", "5"});
        FAIL() << "Expected LuaScriptException for root path";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_PATH path cannot be root"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, NumericOverflowPositive) {
    json initial_doc = {{"value", 1.7e308}}; // Close to max double
    set_initial_json(initial_doc);
    try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "1e308"}); // This should overflow
        FAIL() << "Expected LuaScriptException for numeric overflow";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_OVERFLOW"));
    }
}

TEST_F(LuaScriptManagerNumIncrByTest, NumericOverflowNegative) {
    json initial_doc = {{"value", -1.7e308}}; // Close to min double
    set_initial_json(initial_doc);
    try {
        script_manager_.execute_script("json_numincrby", {test_key_}, {"value", "-1e308"}); // This should overflow
        FAIL() << "Expected LuaScriptException for numeric overflow";
    } catch (const LuaScriptException& e) {
        EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_OVERFLOW"));
    }
}


// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }

// --- Tests for JSON_CLEAR_LUA ---
class LuaScriptManagerJsonClearTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:jsonclear";

    void SetUp() override {
        LuaScriptManagerTest::SetUp(); // Call base SetUp
        if (live_redis_available_) {
            try {
                script_manager_.preload_builtin_scripts();
                ASSERT_TRUE(script_manager_.is_script_loaded("json_clear"));
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);
            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping JsonClear tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping JsonClear tests, live Redis required.";
        }
    }

    void TearDown() override {
        if (live_redis_available_) {
            try {
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                 if (reply) freeReplyObject(reply);
            } catch (...) { /* ignore cleanup errors */ }
        }
        LuaScriptManagerTest::TearDown();
    }

    void set_initial_json(const json& doc) {
        auto conn = conn_manager_.get_connection();
        std::string doc_str = doc.dump();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("SET %s %s", test_key_.c_str(), doc_str.c_str())));
        ASSERT_NE(reply, nullptr) << "Failed to SET initial JSON for test. Context: " << (conn_manager_.get_connection()->get_context() ? conn_manager_.get_connection()->get_context()->errstr : "N/A");
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS) << "SET command failed: " << (reply->str ? reply->str : "Unknown error");
        ASSERT_STREQ(reply->str, "OK");
    }

    json get_current_json() {
        auto conn = conn_manager_.get_connection();
        RedisReplyPtr reply(static_cast<redisReply*>(conn->command("GET %s", test_key_.c_str())));
        if (!reply || reply->type == REDIS_REPLY_NIL) {
            return json(nullptr); // Key does not exist or no value
        }
        if (reply->type != REDIS_REPLY_STRING) {
            throw std::runtime_error("GET command did not return a string for key " + test_key_);
        }
        return json::parse(std::string(reply->str, reply->len));
    }
};

TEST_F(LuaScriptManagerJsonClearTest, ClearRootArray) {
    json initial_doc = {"a", "b", 123};
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // 1 array cleared
    EXPECT_EQ(get_current_json(), json::array());
}

TEST_F(LuaScriptManagerJsonClearTest, ClearEmptyRootArray) {
    set_initial_json(json::array());
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0); // Already empty
    EXPECT_EQ(get_current_json(), json::array());
}

TEST_F(LuaScriptManagerJsonClearTest, ClearRootObject) {
    json initial_doc = {
        {"name", "test"},
        {"count", 100},
        {"active", true},
        {"details", {{"value", 200}, {"items", {1,2}}}}
    };
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    // count: 100->0 (1), details.value: 200->0 (1), details.items: [1,2]->[] (1) = Total 3
    EXPECT_EQ(result.get<long long>(), 3);

    json expected_doc = {
        {"name", "test"}, // string untouched
        {"count", 0},     // number to 0
        {"active", true}, // boolean untouched
        {"details", {{"value", 0}, {"items", json::array()}}}
    };
    EXPECT_EQ(get_current_json(), expected_doc);
}

TEST_F(LuaScriptManagerJsonClearTest, ClearEmptyRootObject) {
    set_initial_json(json::object());
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0);
    EXPECT_EQ(get_current_json(), json::object());
}

TEST_F(LuaScriptManagerJsonClearTest, ClearNestedArray) {
    json initial_doc = {{"data", {"list", {1,2,3}}}};
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"data.list"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // The array 'list' itself is cleared
    json expected_doc = {{"data", {"list", json::array()}}};
    EXPECT_EQ(get_current_json(), expected_doc);
}

TEST_F(LuaScriptManagerJsonClearTest, ClearNestedObject) {
    json initial_doc = {{"config", {{"retries", 5}, {"timeout", 5000}, {"ports", {80,443}}}}};
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"config"});
    ASSERT_TRUE(result.is_number_integer());
    // retries: 5->0 (1), timeout: 5000->0 (1), ports: [80,443]->[] (1) = Total 3
    EXPECT_EQ(result.get<long long>(), 3);
    json expected_doc = {{"config", {{"retries", 0}, {"timeout", 0}, {"ports", json::array()}}}};
    EXPECT_EQ(get_current_json(), expected_doc);
}

TEST_F(LuaScriptManagerJsonClearTest, PathToScalarNumber) {
    set_initial_json({{"value", 123}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"value"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // Scalar "cleared" (touched)
    EXPECT_EQ(get_current_json()["value"], 123); // Value unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, PathToScalarString) {
    set_initial_json({{"text", "hello"}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"text"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // Scalar "cleared"
    EXPECT_EQ(get_current_json()["text"], "hello"); // Value unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, PathToScalarBoolean) {
    set_initial_json({{"flag", true}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"flag"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // Scalar "cleared"
    EXPECT_EQ(get_current_json()["flag"], true); // Value unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, PathToNull) {
    set_initial_json({{"maybe", nullptr}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"maybe"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // Scalar "cleared"
    EXPECT_TRUE(get_current_json()["maybe"].is_null()); // Value unchanged
}


TEST_F(LuaScriptManagerJsonClearTest, PathNotFoundInObject) {
    set_initial_json({{"a", 1}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"b"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0);
    EXPECT_EQ(get_current_json()["a"], 1); // Original doc unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, KeyNotFoundRootPath) {
    // test_key_ is deleted in SetUp
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0);
}

TEST_F(LuaScriptManagerJsonClearTest, KeyNotFoundNonRootPath) {
    // test_key_ is deleted in SetUp
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_clear", {test_key_}, {"some.path"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR document not found"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerJsonClearTest, MalformedJsonDocument) {
    auto conn = conn_manager_.get_connection();
    redisReply* reply_set = conn->command("SET %s %s", test_key_.c_str(), "this is not json {");
    ASSERT_NE(reply_set, nullptr); freeReplyObject(reply_set);

    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_clear", {test_key_}, {"$"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_DECODE"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerJsonClearTest, InvalidPathSyntax) {
    set_initial_json({{"a",1}});
    EXPECT_THROW({
        try {
            script_manager_.execute_script("json_clear", {test_key_}, {"a..b"});
        } catch (const LuaScriptException& e) {
            EXPECT_THAT(e.what(), ::testing::HasSubstr("ERR_PATH"));
            throw;
        }
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerJsonClearTest, ClearObjectWithOnlyNonClearableFields) {
    json initial_doc = {
        {"name", "stringval"},
        {"active", false},
        {"nothing", nullptr}
    };
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0); // No numbers to set to 0, no arrays to empty
    EXPECT_EQ(get_current_json(), initial_doc); // Document should be unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, ClearObjectWithEmptyNestedArrayAndObject) {
    json initial_doc = {
        {"num", 10},
        {"empty_arr_val", json::array()},
        {"empty_obj_val", json::object()}
    };
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    ASSERT_TRUE(result.is_number_integer());
    // num: 10->0 (1)
    // empty_arr_val: []->[] (0, as it was already empty)
    // empty_obj_val: {}->{} (0, as it was already empty and no numbers)
    // Total expected: 1
    EXPECT_EQ(result.get<long long>(), 1);

    json expected_doc = {
        {"num", 0},
        {"empty_arr_val", json::array()},
        {"empty_obj_val", json::object()}
    };
    EXPECT_EQ(get_current_json(), expected_doc);
}
