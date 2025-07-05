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
    std::string test_prefix = "redisjson_test:client:";

    void SetUp() override {
        client_config.host = "127.0.0.1";
        client_config.port = 6379;
        client_config.password = "";
        client_config.database = 0; // Use default DB for tests, ensure it's clean or prefixed
        client_config.connection_pool_size = 3;
        client_config.timeout = std::chrono::milliseconds(500);

        if (!isRedisAvailable()) {
            GTEST_SKIP() << "Redis server not available at " << client_config.host << ":" << client_config.port << ". Skipping all RedisJSONClient tests.";
            return; // Skip further setup if Redis is not available
        }

        client = std::make_unique<redisjson::RedisJSONClient>(client_config);
        cleanupTestKeys(); // Clean up keys from previous runs before each test
    }

    void TearDown() override {
        if (client) { // Only cleanup if client was initialized (Redis was available)
            cleanupTestKeys();
        }
    }

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

    // Helper to clean up test keys
    void cleanupTestKeys() {
        if (!client) return; // Client might not be initialized if Redis was down
        try {
            std::unique_ptr<redisjson::RedisConnection> conn = client->get_redis_connection(); // Need direct access or a helper in client

            // This is a bit of a hack, ideally RedisJSONClient would have a "keys_by_pattern"
            // For now, using raw commands for cleanup.
            std::string pattern = test_prefix + "*";
            redisReply* reply_keys = conn->command("KEYS %s", pattern.c_str());
            if (reply_keys && reply_keys->type == REDIS_REPLY_ARRAY) {
                for (size_t i = 0; i < reply_keys->elements; ++i) {
                    if (reply_keys->element[i]->type == REDIS_REPLY_STRING) {
                        conn->command("DEL %s", reply_keys->element[i]->str);
                    }
                }
            }
            if(reply_keys) freeReplyObject(reply_keys);
            // How to return connection if client->get_redis_connection() is used?
            // For now, assume this connection is single-use for cleanup or client manages it.
            // This needs RedisConnectionManager to be accessible or a specific cleanup method.
            // Let's assume the connection is returned automatically when conn goes out of scope if get_redis_connection
            // was from a pool that uses RAII for return.
            // The current RedisJSONClient::get_redis_connection returns a unique_ptr that needs manual return.
            // This cleanup function needs direct access to connection_manager for proper return.
            // For simplicity in test, we'll leak this cleanup connection or rely on manager's dtor.
            // This is not ideal. A better way: client should provide a way to execute raw commands or have a cleanup util.
        } catch (const std::exception& e) {
            // Ignore errors during cleanup, test will fail if setup fails.
            // std::cerr << "Error during test key cleanup: " << e.what() << std::endl;
        }
    }


    // Helper for direct Redis connection for cleanup (simplification)
    std::unique_ptr<redisjson::RedisConnection> getDirectConnectionForCleanup() {
        auto direct_conn = std::make_unique<redisjson::RedisConnection>(
            client_config.host, client_config.port, client_config.password,
            client_config.database, client_config.timeout);
        if (direct_conn->connect()) {
            return direct_conn;
        }
        return nullptr;
    }

    // Revised cleanup using direct connection
    void cleanupTestKeysRevised() {
        std::unique_ptr<redisjson::RedisConnection> conn = getDirectConnectionForCleanup();
        if (!conn) return;

        std::string pattern = test_prefix + "*";
        redisReply* reply_keys = conn->command("KEYS %s", pattern.c_str());

        if (reply_keys && reply_keys->type == REDIS_REPLY_ARRAY) {
            if (reply_keys->elements > 0) {
                // Construct DEL command with multiple arguments
                std::vector<const char*> argv;
                argv.push_back("DEL");
                std::vector<std::string> key_strings; // to keep c_str valid

                for (size_t i = 0; i < reply_keys->elements; ++i) {
                    if (reply_keys->element[i]->type == REDIS_REPLY_STRING) {
                        key_strings.push_back(reply_keys->element[i]->str);
                        argv.push_back(key_strings.back().c_str());
                    }
                }
                if (argv.size() > 1) {
                     redisReply* del_reply = conn->command_argv(argv.size(), argv.data(), NULL);
                     if (del_reply) freeReplyObject(del_reply);
                }
            }
        }
        if(reply_keys) freeReplyObject(reply_keys);
    }

    // Override SetUp and TearDown to use revised cleanup
    void SetUpRevised() {
        client_config.host = "127.0.0.1";
        client_config.port = 6379; // ... (rest of config)

        if (!isRedisAvailable()) {
            GTEST_SKIP() << "Redis server not available. Skipping all RedisJSONClient tests.";
            return;
        }
        client = std::make_unique<redisjson::RedisJSONClient>(client_config);
        cleanupTestKeysRevised();
    }
    void TearDownRevised() {
        if (client) {
             cleanupTestKeysRevised();
        }
    }
    // To use these, the test fixture would need to call them, or inherit differently.
    // For now, sticking to original SetUp/TearDown using the client's connection manager if possible,
    // or accepting the cleanup limitation. The revised cleanup is better. I'll use it in SetUp/TearDown.
    // Re-integrating revised cleanup:
    void SetUp() override { // This overrides the ::Test::SetUp
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
