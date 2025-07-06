#include "gtest/gtest.h"
#include "redisjson++/redis_json_client.h"
#include "redisjson++/exceptions.h"
#include <thread> // For std::this_thread::sleep_for
#include <chrono> // For std::chrono
#include <iostream> // For GTEST_SKIP logging

// Test fixture for RedisJSONClient tests
class RedisJSONClientTest : public ::testing::Test {
protected:
    redisjson::ClientConfig client_config;
    std::unique_ptr<redisjson::RedisJSONClient> client;
    std::string test_prefix = "redisjson_test:client:"; // This prefix can be removed if FLUSHDB is used.

    // Helper to check Redis availability (copied from ConnectionManagerTest)
    bool isRedisAvailable() {
        redisContext *c = redisConnectWithTimeout(client_config.host.c_str(), client_config.port, {1,0});
        if (c == NULL || c->err) {
            if (c) redisFree(c);
            return false;
        }
        redisReply *reply = (redisReply*)redisCommand(c, "PING");
        bool available = (reply != NULL && reply->type == REDIS_REPLY_STATUS && (strcmp(reply->str,"PONG")==0 || strcmp(reply->str,"OK")==0));
        if (reply) freeReplyObject(reply);
        redisFree(c);
        return available;
    }

    // Helper for direct Redis connection (used for setup/teardown FLUSHDB)
    std::unique_ptr<redisjson::RedisConnection> getDirectConnectionForCleanup() {
        // Ensure database is 0 for initial connection if SELECT is used,
        // or rely on client_config.database for the actual test DB.
        // For FLUSHDB, it usually applies to the currently selected DB.
        auto direct_conn = std::make_unique<redisjson::RedisConnection>(
            client_config.host, client_config.port, client_config.password,
            client_config.database, // Connect to the target test database directly
            client_config.timeout);
        if (direct_conn->connect()) {
            return direct_conn;
        }
        // Fallback to DB 0 if connection to specific DB failed and then select?
        // No, if connect to test DB fails, it's an issue.
        return nullptr;
    }

    // The active SetUp and TearDown using FLUSHDB on a dedicated test database (e.g., 15)
    void SetUp() override {
        client_config.host = "127.0.0.1";
        client_config.port = 6379;
        client_config.password = "";
        client_config.database = 15; // Use a specific DB for tests like 15
        client_config.connection_pool_size = 3;
        client_config.timeout = std::chrono::milliseconds(500);

        if (!isRedisAvailable()) {
            GTEST_SKIP() << "Redis server not available at " << client_config.host << ":" << client_config.port << ". Skipping all RedisJSONClient tests.";
            return;
        }

        // Select DB 15 for tests then clean
        std::unique_ptr<redisjson::RedisConnection> temp_conn = getDirectConnectionForCleanup();
        if (temp_conn) {
            redisReply* reply = temp_conn->command("SELECT %d", client_config.database); // Select test DB
            if (reply) freeReplyObject(reply);
            reply = temp_conn->command("FLUSHDB"); // Clear the test DB
            if (reply) freeReplyObject(reply);
        } else {
             GTEST_SKIP() << "Failed to connect to Redis for DB setup. Skipping tests.";
             return;
        }

        client = std::make_unique<redisjson::RedisJSONClient>(client_config);
        // No need for cleanupTestKeysRevised in SetUp if FLUSHDB is used.
    }

    void TearDown() override {
        // Optional: could FLUSHDB again in TearDown if desired, but usually not necessary if SetUp does it.
        if (client && isRedisAvailable()) { // Ensure Redis is still there for teardown
             std::unique_ptr<redisjson::RedisConnection> temp_conn = getDirectConnectionForCleanup();
             if (temp_conn) {
                redisReply* reply = temp_conn->command("SELECT %d", client_config.database);
                if (reply) freeReplyObject(reply);
                reply = temp_conn->command("FLUSHDB");
                if (reply) freeReplyObject(reply);
            }
        }
    }


};


TEST_F(RedisJSONClientTest, SetAndGetJson) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "doc1";
    json doc_to_set = {{"name", "John Doe"}, {"age", 30}, {"isStudent", false}};

    ASSERT_NO_THROW(client->set_json(key, doc_to_set));

    json retrieved_doc;
    ASSERT_NO_THROW(retrieved_doc = client->get_json(key));

    EXPECT_EQ(doc_to_set, retrieved_doc);
}

TEST_F(RedisJSONClientTest, GetJsonNonExistentKey) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "nonexistent";
    ASSERT_THROW(client->get_json(key), redisjson::PathNotFoundException);
}

TEST_F(RedisJSONClientTest, ExistsJson) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key_exists = test_prefix + "exists1";
    const std::string key_not_exists = test_prefix + "exists_not";
    json doc = {{"value", 123}};

    client->set_json(key_exists, doc);

    bool exists = false;
    ASSERT_NO_THROW(exists = client->exists_json(key_exists));
    EXPECT_TRUE(exists);

    bool not_exists = true;
    ASSERT_NO_THROW(not_exists = client->exists_json(key_not_exists));
    EXPECT_FALSE(not_exists);
}

TEST_F(RedisJSONClientTest, DelJson) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "del1";
    json doc = {{"temp", "data"}};

    client->set_json(key, doc);
    bool exists_before_del = false;
    ASSERT_NO_THROW(exists_before_del = client->exists_json(key));
    EXPECT_TRUE(exists_before_del);

    ASSERT_NO_THROW(client->del_json(key));

    bool exists_after_del = true;
    ASSERT_NO_THROW(exists_after_del = client->exists_json(key));
    EXPECT_FALSE(exists_after_del);
}

TEST_F(RedisJSONClientTest, SetJsonWithTTL) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "ttl_doc";
    json doc_to_set = {{"item", "expiring"}};
    redisjson::SetOptions opts;
    opts.ttl = std::chrono::seconds(1); // Expires in 1 second

    ASSERT_NO_THROW(client->set_json(key, doc_to_set, opts));

    // Check it exists immediately
    bool exists_immediately = false;
    ASSERT_NO_THROW(exists_immediately = client->exists_json(key));
    EXPECT_TRUE(exists_immediately);

    // Wait for TTL to expire (plus a little buffer)
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Check it no longer exists
    bool exists_after_ttl = true;
    ASSERT_NO_THROW(exists_after_ttl = client->exists_json(key));
    EXPECT_FALSE(exists_after_ttl);
}

TEST_F(RedisJSONClientTest, GetMalformedJson) {
     if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "malformed_json";

    // Manually set a non-JSON string using a direct connection
    std::unique_ptr<redisjson::RedisConnection> conn = getDirectConnectionForCleanup();
    ASSERT_NE(conn, nullptr) << "Failed to get direct connection for test setup";
    conn->command("SET %s %s", key.c_str(), "this is not json");

    ASSERT_THROW(client->get_json(key), redisjson::JsonParsingException);
}


// --- Path Operations Tests ---
TEST_F(RedisJSONClientTest, SetAndGetPath) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "path_doc1";
    json initial_doc = {
        {"name", "Alice"},
        {"age", 30},
        {"address", {
            {"street", "123 Main St"},
            {"city", "Wonderland"}
        }},
        {"tags", json::array({"friendly", "coder"})}
    };
    client->set_json(key, initial_doc);

    // Test setting a new scalar value
    json new_age = 31;
    ASSERT_NO_THROW(client->set_path(key, "$.age", new_age));
    json retrieved_age_array; // JSON.GET path returns an array with the single value
    ASSERT_NO_THROW(retrieved_age_array = client->get_path(key, "$.age"));
    ASSERT_TRUE(retrieved_age_array.is_array() && retrieved_age_array.size() == 1);
    EXPECT_EQ(retrieved_age_array[0], new_age);

    // Test setting a new object value
    json new_city = {{"city", "New Wonderland"}}; // Note: JSON.SET path value replaces the target.
                                               // So this replaces the string "Wonderland" with object {"city": "New Wonderland"}
                                               // This is probably not what one intends for $.address.city.
                                               // The value for path should be the actual value, not an object containing it.
    json new_city_value = "New Wonderland";
    ASSERT_NO_THROW(client->set_path(key, "$.address.city", new_city_value));
    json retrieved_city_array;
    ASSERT_NO_THROW(retrieved_city_array = client->get_path(key, "$.address.city"));
    ASSERT_TRUE(retrieved_city_array.is_array() && retrieved_city_array.size() == 1);
    EXPECT_EQ(retrieved_city_array[0], new_city_value);
    
    // Test getting a path that returns multiple values (e.g. wildcard, not directly tested here with simple path)
    // For a simple path like "$.name", JSON.GET returns an array of one element.
    json name_val_array;
    ASSERT_NO_THROW(name_val_array = client->get_path(key, "$.name"));
    ASSERT_TRUE(name_val_array.is_array() && name_val_array.size() == 1);
    EXPECT_EQ(name_val_array[0], "Alice");

    // Test getting a non-existent path
    ASSERT_THROW(client->get_path(key, "$.nonexistent"), redisjson::PathNotFoundException);

    // Test set_path with NX (should succeed as path $.newfield does not exist)
    redisjson::SetOptions opts_nx;
    opts_nx.condition = redisjson::SetCmdCondition::NX;
    json new_field_val = "new value";
    ASSERT_NO_THROW(client->set_path(key, "$.newfield", new_field_val, opts_nx));
    json new_field_ret_array;
    ASSERT_NO_THROW(new_field_ret_array = client->get_path(key, "$.newfield"));
    ASSERT_TRUE(new_field_ret_array.is_array() && new_field_ret_array.size() == 1);
    EXPECT_EQ(new_field_ret_array[0], new_field_val);

    // Test set_path with NX (should do nothing as path $.newfield now exists)
    json newer_field_val = "newer value";
    ASSERT_NO_THROW(client->set_path(key, "$.newfield", newer_field_val, opts_nx)); // Should not throw, but also not update.
    ASSERT_NO_THROW(new_field_ret_array = client->get_path(key, "$.newfield"));
    ASSERT_TRUE(new_field_ret_array.is_array() && new_field_ret_array.size() == 1);
    EXPECT_EQ(new_field_ret_array[0], new_field_val); // Check it's still the old value

    // Test set_path with XX (should succeed as path $.newfield exists)
    redisjson::SetOptions opts_xx;
    opts_xx.condition = redisjson::SetCmdCondition::XX;
    ASSERT_NO_THROW(client->set_path(key, "$.newfield", newer_field_val, opts_xx));
    ASSERT_NO_THROW(new_field_ret_array = client->get_path(key, "$.newfield"));
    ASSERT_TRUE(new_field_ret_array.is_array() && new_field_ret_array.size() == 1);
    EXPECT_EQ(new_field_ret_array[0], newer_field_val); // Check it's the new value

    // Test set_path with XX (should do nothing as path $.anotherNew does not exist)
    json another_new_val = "another";
    ASSERT_NO_THROW(client->set_path(key, "$.anotherNew", another_new_val, opts_xx));
    ASSERT_THROW(client->get_path(key, "$.anotherNew"), redisjson::PathNotFoundException); // Verify it wasn't created
}

TEST_F(RedisJSONClientTest, DelAndExistsPath) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "path_doc2";
    json doc = {
        {"user", {
            {"name", "Bob"},
            {"status", "active"}
        }},
        {"item", "test_item"}
    };
    client->set_json(key, doc);

    // Check initial existence
    bool p_exists = false;
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.user.status"));
    EXPECT_TRUE(p_exists);
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.item"));
    EXPECT_TRUE(p_exists);
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.user.nonexistent"));
    EXPECT_FALSE(p_exists);
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.non_root_path")); // Path on non-existent root
    EXPECT_FALSE(p_exists);


    // Delete path $.user.status
    ASSERT_NO_THROW(client->del_path(key, "$.user.status"));
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.user.status"));
    EXPECT_FALSE(p_exists);
    ASSERT_THROW(client->get_path(key, "$.user.status"), redisjson::PathNotFoundException); // Verify get fails

    // Check other paths still exist
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.user.name"));
    EXPECT_TRUE(p_exists);
    json name_val_array;
    ASSERT_NO_THROW(name_val_array = client->get_path(key, "$.user.name"));
    ASSERT_TRUE(name_val_array.is_array() && name_val_array.size() == 1);
    EXPECT_EQ(name_val_array[0], "Bob");


    // Delete a root path $.item
    ASSERT_NO_THROW(client->del_path(key, "$.item"));
    ASSERT_NO_THROW(p_exists = client->exists_path(key, "$.item"));
    EXPECT_FALSE(p_exists);

    // Delete non-existent path (should not throw)
    ASSERT_NO_THROW(client->del_path(key, "$.nonexistent.path"));

    // Delete path on non-existent key (should not throw error, but DEL returns 0)
    ASSERT_NO_THROW(client->del_path("nonexistent_key_for_del_path", "$.some.path"));
}


// --- Array Operations Tests ---
TEST_F(RedisJSONClientTest, ArrayOperations) {
    if (!client) GTEST_SKIP() << "Client not initialized, skipping test.";
    const std::string key = test_prefix + "array_doc1";
    json initial_doc = { {"my_array", {1, 2, 3}} };
    client->set_json(key, initial_doc);

    // Length
    size_t len = 0;
    ASSERT_NO_THROW(len = client->array_length(key, "$.my_array"));
    EXPECT_EQ(len, 3);

    // Append
    ASSERT_NO_THROW(client->append_path(key, "$.my_array", 4)); // Append single value
    ASSERT_NO_THROW(len = client->array_length(key, "$.my_array"));
    EXPECT_EQ(len, 4);
    json arr_val_array;
    ASSERT_NO_THROW(arr_val_array = client->get_path(key, "$.my_array")); // $.my_array returns [ [1,2,3,4] ]
    ASSERT_TRUE(arr_val_array.is_array() && arr_val_array.size() == 1);
    EXPECT_EQ(arr_val_array[0], json({1,2,3,4}));


    // Prepend
    ASSERT_NO_THROW(client->prepend_path(key, "$.my_array", 0)); // Prepend single value
    ASSERT_NO_THROW(len = client->array_length(key, "$.my_array"));
    EXPECT_EQ(len, 5);
    ASSERT_NO_THROW(arr_val_array = client->get_path(key, "$.my_array"));
    ASSERT_TRUE(arr_val_array.is_array() && arr_val_array.size() == 1);
    EXPECT_EQ(arr_val_array[0], json({0,1,2,3,4}));

    // Pop from back (default index -1)
    json popped_val;
    ASSERT_NO_THROW(popped_val = client->pop_path(key, "$.my_array")); // Default index -1
    EXPECT_EQ(popped_val, 4);
    ASSERT_NO_THROW(len = client->array_length(key, "$.my_array"));
    EXPECT_EQ(len, 4);
    ASSERT_NO_THROW(arr_val_array = client->get_path(key, "$.my_array"));
    ASSERT_TRUE(arr_val_array.is_array() && arr_val_array.size() == 1);
    EXPECT_EQ(arr_val_array[0], json({0,1,2,3}));


    // Pop from front (index 0)
    ASSERT_NO_THROW(popped_val = client->pop_path(key, "$.my_array", 0));
    EXPECT_EQ(popped_val, 0);
    ASSERT_NO_THROW(len = client->array_length(key, "$.my_array"));
    EXPECT_EQ(len, 3);

    // Pop from middle (index 1, which is value 2)
    ASSERT_NO_THROW(client->set_path(key, "$.my_array", json({10,20,30,40}))); // Reset array
    ASSERT_NO_THROW(popped_val = client->pop_path(key, "$.my_array", 1));
    EXPECT_EQ(popped_val, 20);
    ASSERT_NO_THROW(arr_val_array = client->get_path(key, "$.my_array"));
    ASSERT_TRUE(arr_val_array.is_array() && arr_val_array.size() == 1);
    EXPECT_EQ(arr_val_array[0], json({10,30,40}));


    // Error cases
    ASSERT_THROW(client->array_length(key, "$.non_array_path"), redisjson::PathNotFoundException); // Path not an array or not found
    client->set_json(key, {{"not_an_array", 123}});
    ASSERT_THROW(client->append_path(key, "$.not_an_array", 5), redisjson::TypeMismatchException); // Corrected expected exception
    ASSERT_THROW(client->array_length(key, "$.not_an_array"), redisjson::PathNotFoundException); // Path not an array for length check
    ASSERT_THROW(client->pop_path(key, "$.not_an_array",0), redisjson::PathNotFoundException); // Corrected expected exception
}


// This main is not strictly needed if CMake handles gtest_discover_tests and links GTest::gtest_main
// However, it's common to include it.
// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }
