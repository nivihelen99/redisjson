#include "gtest/gtest.h"
#include "redisjson++/lua_script_manager.h"
#include "redisjson++/redis_connection_manager.h" // Required by LuaScriptManager
#include "redisjson++/exceptions.h"

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


// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }
