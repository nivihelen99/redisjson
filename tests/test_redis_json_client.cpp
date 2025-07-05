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

// This main is not strictly needed if CMake handles gtest_discover_tests and links GTest::gtest_main
// However, it's common to include it.
// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }
