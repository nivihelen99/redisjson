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

    // Helper function to create a ClientConfig with a short timeout
    static ClientConfig CreateTestClientConfig() {
        ClientConfig config;
        config.timeout = std::chrono::milliseconds(200);
        // Potentially set host/port here if not default, e.g. from env vars for testing flexibility
        return config;
    }

    LuaScriptManagerTest() :
        client_config_(CreateTestClientConfig()), // Initialize with short timeout
        conn_manager_(client_config_),           // conn_manager uses this config
        script_manager_(&conn_manager_)          // script_manager uses the conn_manager
    {
        // Basic check if Redis might be running for more meaningful tests
        try {
        // Use a new RedisConnection with the short timeout for the check
        redisjson::RedisConnection test_conn(
            client_config_.host,
            client_config_.port,
            client_config_.password,
            client_config_.database,
            client_config_.timeout // This is already the short timeout
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

// --- Tests for JSON_ARRAY_TRIM_LUA ---
class LuaScriptManagerArrTrimTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:arrtrim";

    void SetUp() override {
        LuaScriptManagerTest::SetUp(); // Call base SetUp
        if (live_redis_available_) {
            try {
                script_manager_.preload_builtin_scripts();
                ASSERT_TRUE(script_manager_.is_script_loaded("json_array_trim"));
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);
            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping ArrTrim tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping ArrTrim tests, live Redis required.";
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
        ASSERT_NE(reply, nullptr);
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
        ASSERT_STREQ(reply->str, "OK");
    }

    json get_current_json_at_path(const std::string& path = "$") {
        auto conn = conn_manager_.get_connection();
        RedisReplyPtr reply_get(static_cast<redisReply*>(conn->command("GET %s", test_key_.c_str())));
        if (!reply_get || reply_get->type == REDIS_REPLY_NIL) return json(nullptr);
        if (reply_get->type != REDIS_REPLY_STRING) throw std::runtime_error("GET failed in test verification");

        json full_doc = json::parse(std::string(reply_get->str, reply_get->len));
        if (path == "$" || path == "") return full_doc;

        // Basic path navigation for test verification (not using PathParser for simplicity here)
        // This is a very simplified path resolver for testing, assuming dot notation and simple indices.
        json current = full_doc;
        std::string p = path;
        if (p.rfind("$.", 0) == 0) p = p.substr(2); // Remove $. prefix
        else if (p.rfind("$[", 0) == 0) p = p.substr(1); // Remove $ prefix for root array e.g. $[0] -> [0]

        size_t start_pos = 0;
        while(start_pos < p.length()){
            size_t dot_pos = p.find('.', start_pos);
            size_t bracket_pos = p.find('[', start_pos);
            std::string segment;

            if (dot_pos != std::string::npos && (bracket_pos == std::string::npos || dot_pos < bracket_pos)) {
                segment = p.substr(start_pos, dot_pos - start_pos);
                start_pos = dot_pos + 1;
                if (!current.is_object() || !current.contains(segment)) return json(nullptr); // Path segment not found
                current = current[segment];
            } else if (bracket_pos != std::string::npos && (dot_pos == std::string::npos || bracket_pos < dot_pos)) {
                if (bracket_pos > start_pos) { // Key before bracket, e.g. obj[0]
                     segment = p.substr(start_pos, bracket_pos - start_pos);
                     if (!current.is_object() || !current.contains(segment)) return json(nullptr);
                     current = current[segment];
                }
                size_t end_bracket_pos = p.find(']', bracket_pos);
                if (end_bracket_pos == std::string::npos) return json(nullptr); // Malformed
                std::string index_str = p.substr(bracket_pos + 1, end_bracket_pos - bracket_pos - 1);
                try {
                    int index = std::stoi(index_str);
                    if (!current.is_array() || static_cast<size_t>(index) >= current.size() || index < 0) return json(nullptr);
                    current = current[index];
                } catch (const std::exception&) { return json(nullptr); /* Not a number */ }
                start_pos = end_bracket_pos + 1;
                if (start_pos < p.length() && p[start_pos] == '.') start_pos++; // Consume dot after bracket if present
            } else {
                segment = p.substr(start_pos);
                if (!current.is_object() || !current.contains(segment)) return json(nullptr);
                current = current[segment];
                break;
            }
        }
        return current;
    }

    long long execute_arrtrim_script(const std::string& path, long long start_idx, long long stop_idx) {
        json result = script_manager_.execute_script("json_array_trim", {test_key_}, {path, std::to_string(start_idx), std::to_string(stop_idx)});
        if (!result.is_number_integer()) {
            throw std::runtime_error("ARRTRIM script did not return an integer. Got: " + result.dump());
        }
        return result.get<long long>();
    }
};

TEST_F(LuaScriptManagerArrTrimTest, PositiveIndices) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}});
    EXPECT_EQ(execute_arrtrim_script("arr", 1, 3), 3);
    EXPECT_EQ(get_current_json_at_path("arr"), json({1,2,3}));
}

TEST_F(LuaScriptManagerArrTrimTest, NegativeStartIndex) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}}); // len 6
    EXPECT_EQ(execute_arrtrim_script("arr", -3, 4), 2); // -3 is index 3. Trim [3,4] -> [3,4]
    EXPECT_EQ(get_current_json_at_path("arr"), json({3,4}));
}

TEST_F(LuaScriptManagerArrTrimTest, NegativeStopIndex) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}}); // len 6
    EXPECT_EQ(execute_arrtrim_script("arr", 1, -2), 4); // -2 is index 4. Trim [1,4] -> [1,2,3,4]
    EXPECT_EQ(get_current_json_at_path("arr"), json({1,2,3,4}));
}

TEST_F(LuaScriptManagerArrTrimTest, BothNegativeIndices) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}}); // len 6
    EXPECT_EQ(execute_arrtrim_script("arr", -4, -2), 3); // -4 is index 2, -2 is index 4. Trim [2,4] -> [2,3,4]
    EXPECT_EQ(get_current_json_at_path("arr"), json({2,3,4}));
}

TEST_F(LuaScriptManagerArrTrimTest, StartGreaterThanStop) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}});
    EXPECT_EQ(execute_arrtrim_script("arr", 3, 1), 0);
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
}

TEST_F(LuaScriptManagerArrTrimTest, StartEqualsStop) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}});
    EXPECT_EQ(execute_arrtrim_script("arr", 2, 2), 1);
    EXPECT_EQ(get_current_json_at_path("arr"), json({2}));
}

TEST_F(LuaScriptManagerArrTrimTest, StartOutOfBoundsLargePositive) {
    set_initial_json(json{{"arr", {0,1,2}}}); // len 3
    // start=10 (normalized to 3), stop=12 (normalized to 2). start(3) > stop(2) -> empty
    EXPECT_EQ(execute_arrtrim_script("arr", 10, 12), 0);
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
}

TEST_F(LuaScriptManagerArrTrimTest, StopOutOfBoundsLargePositive) {
    set_initial_json(json{{"arr", {0,1,2,3,4}}}); // len 5
    // start=1, stop=10 (normalized to 4). Trim [1,4] -> [1,2,3,4]
    EXPECT_EQ(execute_arrtrim_script("arr", 1, 10), 4);
    EXPECT_EQ(get_current_json_at_path("arr"), json({1,2,3,4}));
}

TEST_F(LuaScriptManagerArrTrimTest, StartOutOfBoundsLargeNegative) {
    set_initial_json(json{{"arr", {0,1,2}}}); // len 3
    // start=-10 (normalized to 0), stop=1. Trim [0,1] -> [0,1]
    EXPECT_EQ(execute_arrtrim_script("arr", -10, 1), 2);
    EXPECT_EQ(get_current_json_at_path("arr"), json({0,1}));
}

TEST_F(LuaScriptManagerArrTrimTest, StopOutOfBoundsLargeNegative) {
    set_initial_json(json{{"arr", {0,1,2}}}); // len 3
    // start=1, stop=-10 (normalized to -1). start(1) > stop(-1) -> empty
    EXPECT_EQ(execute_arrtrim_script("arr", 1, -10), 0);
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
}

TEST_F(LuaScriptManagerArrTrimTest, TrimEmptyArray) {
    set_initial_json(json{{"arr", json::array()}});
    EXPECT_EQ(execute_arrtrim_script("arr", 0, 0), 0);
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
    EXPECT_EQ(execute_arrtrim_script("arr", 0, 10), 0); // Still empty
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
    EXPECT_EQ(execute_arrtrim_script("arr", -1, -1), 0); // Still empty
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
}

TEST_F(LuaScriptManagerArrTrimTest, TrimRootArray) {
    set_initial_json(json::array({0,1,2,3}));
    EXPECT_EQ(execute_arrtrim_script("$", 1, 2), 2);
    EXPECT_EQ(get_current_json_at_path("$"), json({1,2}));
}

TEST_F(LuaScriptManagerArrTrimTest, ErrorKeyNotFound) {
    EXPECT_THROW({ execute_arrtrim_script("$", 0, 1); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrTrimTest, ErrorPathNotFound) {
    set_initial_json({{"some", "object"}});
    EXPECT_THROW({ execute_arrtrim_script("data.list", 0, 1); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrTrimTest, ErrorNotAnArray) {
    set_initial_json({{"arr", "this is a string"}});
    EXPECT_THROW({ execute_arrtrim_script("arr", 0, 1); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrTrimTest, ErrorInvalidStartIndexString) {
    set_initial_json({{"arr", {1,2,3}}});
    EXPECT_THROW({
        script_manager_.execute_script("json_array_trim", {test_key_}, {"arr", "not_a_number", "1"});
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrTrimTest, ErrorInvalidStopIndexString) {
    set_initial_json({{"arr", {1,2,3}}});
    EXPECT_THROW({
        script_manager_.execute_script("json_array_trim", {test_key_}, {"arr", "0", "not_a_number_either"});
    }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrTrimTest, TrimToExactSameArray) {
    set_initial_json(json{{"arr", {0,1,2,3}}}); // len 4
    EXPECT_EQ(execute_arrtrim_script("arr", 0, 3), 4);
    EXPECT_EQ(get_current_json_at_path("arr"), json({0,1,2,3}));
}

TEST_F(LuaScriptManagerArrTrimTest, TrimWithStopIndexBeforeStartAfterNormalization) {
    set_initial_json(json{{"arr", {0,1,2,3,4,5}}}); // len 6
    // start = 5, stop = -5 (norm: 6-5=1). start(5) > stop(1) -> empty
    EXPECT_EQ(execute_arrtrim_script("arr", 5, -5), 0);
    EXPECT_EQ(get_current_json_at_path("arr"), json::array());
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
    // Ensure initial_doc is {"data": {"list": [1,2,3]}}
    // The previous syntax {{"data", {"list", {1,2,3}}}} might have created {"data": ["list", [1,2,3]]}
    // which would make "data.list" an invalid path to the array [1,2,3].
    json initial_doc = R"({"data":{"list":[1,2,3]}})"_json;
    set_initial_json(initial_doc);
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"data.list"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 1); // The array 'list' itself is cleared
    json expected_doc = R"({"data":{"list":[]}})"_json; // Ensure expected doc matches the new style
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
    EXPECT_EQ(result.get<long long>(), 1); // Number is modified (set to 0), count is 1
    EXPECT_EQ(get_current_json()["value"], 0); // Value becomes 0
}

TEST_F(LuaScriptManagerJsonClearTest, PathToScalarString) {
    set_initial_json({{"text", "hello"}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"text"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0); // String is not modified, count is 0
    EXPECT_EQ(get_current_json()["text"], "hello"); // Value unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, PathToScalarBoolean) {
    set_initial_json({{"flag", true}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"flag"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0); // Boolean is not modified, count is 0
    EXPECT_EQ(get_current_json()["flag"], true); // Value unchanged
}

TEST_F(LuaScriptManagerJsonClearTest, PathToNull) {
    set_initial_json({{"maybe", nullptr}});
    json result = script_manager_.execute_script("json_clear", {test_key_}, {"maybe"});
    ASSERT_TRUE(result.is_number_integer());
    EXPECT_EQ(result.get<long long>(), 0); // Null is not modified, count is 0
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
        script_manager_.execute_script("json_clear", {test_key_}, {"$"});
    }, LuaScriptException); // Just check that it throws, message can vary.
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

// --- Tests for JSON_ARRINDEX_LUA ---
class LuaScriptManagerArrIndexTest : public LuaScriptManagerTest {
protected:
    std::string test_key_ = "luatest:arrindex";

    void SetUp() override {
        LuaScriptManagerTest::SetUp(); // Call base SetUp
        if (live_redis_available_) {
            try {
                script_manager_.preload_builtin_scripts();
                ASSERT_TRUE(script_manager_.is_script_loaded("json_arrindex"));
                auto conn = conn_manager_.get_connection();
                redisReply* reply = conn->command("DEL %s", test_key_.c_str());
                if (reply) freeReplyObject(reply);
            } catch (const std::exception& e) {
                GTEST_SKIP() << "Skipping ArrIndex tests, setup failed: " << e.what();
            }
        } else {
            GTEST_SKIP() << "Skipping ArrIndex tests, live Redis required.";
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
        ASSERT_NE(reply, nullptr);
        ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
        ASSERT_STREQ(reply->str, "OK");
    }

    long long execute_arrindex_script(const std::string& path, const std::string& value_json_str,
                                   const std::optional<std::string>& start_index_str = std::nullopt,
                                   const std::optional<std::string>& end_index_str = std::nullopt) {
        std::vector<std::string> args = {path, value_json_str};
        if (start_index_str.has_value()) {
            args.push_back(start_index_str.value());
        } else {
            args.push_back(""); // Lua script expects placeholders if not provided
        }
        if (end_index_str.has_value()) {
            args.push_back(end_index_str.value());
        } else {
            args.push_back("");
        }
        json result = script_manager_.execute_script("json_arrindex", {test_key_}, args);
        if (!result.is_number_integer()) {
            throw std::runtime_error("ARRINDEX script did not return an integer. Got: " + result.dump());
        }
        return result.get<long long>();
    }
};

TEST_F(LuaScriptManagerArrIndexTest, FindStringValue) {
    set_initial_json(json{{"arr", {"hello", "world", "hello", "again"}}});
    EXPECT_EQ(execute_arrindex_script("arr", R"("world")"), 1);
    EXPECT_EQ(execute_arrindex_script("arr", R"("hello")"), 0); // First occurrence
}

TEST_F(LuaScriptManagerArrIndexTest, FindNumericValue) {
    set_initial_json(json{{"arr", {10, 20.5, 30, 20.5}}});
    EXPECT_EQ(execute_arrindex_script("arr", "20.5"), 1);
    EXPECT_EQ(execute_arrindex_script("arr", "30"), 2);
}

TEST_F(LuaScriptManagerArrIndexTest, FindBooleanValue) {
    set_initial_json(json{{"arr", {true, false, true}}});
    EXPECT_EQ(execute_arrindex_script("arr", "false"), 1);
    EXPECT_EQ(execute_arrindex_script("arr", "true"), 0);
}

TEST_F(LuaScriptManagerArrIndexTest, FindNullValue) {
    set_initial_json(json{{"arr", {"a", nullptr, "b", nullptr}}});
    EXPECT_EQ(execute_arrindex_script("arr", "null"), 1);
}

TEST_F(LuaScriptManagerArrIndexTest, ValueNotFound) {
    set_initial_json(json{{"arr", {"a", "b", "c"}}});
    EXPECT_EQ(execute_arrindex_script("arr", R"("d")"), -1);
}

TEST_F(LuaScriptManagerArrIndexTest, EmptyArray) {
    set_initial_json(json{{"arr", json::array()}});
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")"), -1);
}

TEST_F(LuaScriptManagerArrIndexTest, WithStartIndex) {
    set_initial_json(json{{"arr", {"a", "b", "a", "c"}}});
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "1"), 2); // Search "a" starting from index 1
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "0"), 0);
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "3"), -1); // Start index past last 'a'
}

TEST_F(LuaScriptManagerArrIndexTest, WithStartAndEndIndex) {
    set_initial_json(json{{"arr", {"a", "b", "c", "a", "d", "a"}}}); // indices: 0,1,2,3,4,5
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "1", "4"), 3); // Search "a" in slice [b,c,a,d] (indices 1-4)
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "0", "2"), 0); // Search "a" in slice [a,b,c] (indices 0-2)
    EXPECT_EQ(execute_arrindex_script("arr", R"("c")", "1", "1"), -1); // Search "c" in slice [b]
    EXPECT_EQ(execute_arrindex_script("arr", R"("b")", "1", "1"), 1); // Search "b" in slice [b]
}

TEST_F(LuaScriptManagerArrIndexTest, NegativeStartIndex) {
    set_initial_json(json{{"arr", {"a", "b", "a", "c"}}}); // len 4
    // -1 means last element (index 3, "c"). Search "a" starting from "c". Expected -1
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "-1"), -1);
    // -2 means "a" (index 2). Search "a" starting from "a" (idx 2). Expected 2.
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "-2"), 2);
    // -4 means "a" (index 0). Search "a" starting from "a" (idx 0). Expected 0.
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "-4"), 0);
     // -5 means effectively index 0.
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "-5"), 0);
}

TEST_F(LuaScriptManagerArrIndexTest, NegativeEndIndex) {
    set_initial_json(json{{"arr", {"a", "b", "c", "a", "d"}}}); // len 5. Indices 0,1,2,3,4
    // Search "a" from start (0) up to 2nd last (-2, which is "a" at index 3)
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "0", "-2"), 0);
    // Search "a" from start (0) up to last element (-1, which is "d" at index 4)
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "0", "-1"), 0);
    // Search "d" from start (0) up to last element (-1, "d" at index 4)
    EXPECT_EQ(execute_arrindex_script("arr", R"("d")", "0", "-1"), 4);
}

TEST_F(LuaScriptManagerArrIndexTest, StartIndexAfterEndIndex) {
    set_initial_json(json{{"arr", {"a", "b", "c"}}});
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "2", "1"), -1);
}

TEST_F(LuaScriptManagerArrIndexTest, IndicesOutOfBounds) {
    set_initial_json(json{{"arr", {"a", "b", "c"}}}); // len 3
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "5"), -1); // Start index too high
    EXPECT_EQ(execute_arrindex_script("arr", R"("a")", "0", "10"), 0); // End index too high, clamped
    EXPECT_EQ(execute_arrindex_script("arr", R"("c")", "0", "1"), -1); // 'c' is at index 2, range [0,1]
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorKeyNotFound) {
    EXPECT_THROW({ execute_arrindex_script("$", R"("val")"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorPathNotFound) {
    set_initial_json({{"some", "object"}});
    EXPECT_THROW({ execute_arrindex_script("data.list", R"("val")"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorNotAnArray) {
    set_initial_json({{"arr", "this is a string"}});
    EXPECT_THROW({ execute_arrindex_script("arr", R"("val")"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorInvalidPathSyntax) {
    set_initial_json({{"arr", {1,2}}});
    EXPECT_THROW({ execute_arrindex_script("arr..invalid", R"("val")"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorValueToFindNotJsonScalar) {
    set_initial_json({{"arr", {1,2}}});
    // The Lua script's cjson.decode will error on non-JSON.
    EXPECT_THROW({ execute_arrindex_script("arr", "not_json_value"); }, LuaScriptException);
    // Test with a valid JSON structure that isn't a scalar (though script implies scalar focus)
    // Current script will try to compare, might work for empty object/array if also in target array.
    // For simplicity, client should enforce scalar, but testing script's robustness.
    // EXPECT_EQ(execute_arrindex_script("arr", "{}"), -1); // Assuming {} is not in {1,2}
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorInvalidStartIndex) {
    set_initial_json({{"arr", {1,2}}});
    EXPECT_THROW({ execute_arrindex_script("arr", "1", "not_a_number"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ErrorInvalidEndIndex) {
    set_initial_json({{"arr", {1,2}}});
    EXPECT_THROW({ execute_arrindex_script("arr", "1", "0", "not_a_number_either"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, PathIsRootAndRootIsArray) {
    set_initial_json(json::array({"root_val", 100}));
    EXPECT_EQ(execute_arrindex_script("$", "100"), 1);
    EXPECT_EQ(execute_arrindex_script("$", R"("root_val")"), 0);
    EXPECT_EQ(execute_arrindex_script("$", R"("not_found")"), -1);
}

TEST_F(LuaScriptManagerArrIndexTest, PathIsRootAndRootIsNotArray) {
    set_initial_json({{"key", "value"}}); // Root is an object
    EXPECT_THROW({ execute_arrindex_script("$", R"("value")"); }, LuaScriptException);
}

TEST_F(LuaScriptManagerArrIndexTest, ComplexNestedPathToArray) {
    json doc = {
        {"level1", {
            {"level2", {
                {"my_array", {"find_me", "dont_find"}}
            }}
        }}
    };
    set_initial_json(doc);
    EXPECT_EQ(execute_arrindex_script("level1.level2.my_array", R"("find_me")"), 0);
    EXPECT_EQ(execute_arrindex_script("level1.level2.my_array", R"("find_me")", "0", "0"), 0);
    EXPECT_EQ(execute_arrindex_script("level1.level2.my_array", R"("find_me")", "1", "1"), -1);
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
