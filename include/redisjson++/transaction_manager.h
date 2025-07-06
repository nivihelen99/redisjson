#pragma once

#include "redis_connection_manager.h" // For RedisConnection or similar
#include "exceptions.h"             // For TransactionException
#include "path_parser.h"            // For PathParser if paths need parsing here
#include "json_modifier.h"          // For JSONModifier if ops are complex client-side before queueing
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <memory> // For std::unique_ptr if Transaction is heap-allocated by begin_transaction

using json = nlohmann::json;

namespace redisjson {

// Forward declarations
class RedisConnectionManager;
class RedisConnection;
// class PathParser; // Already included
// class JSONModifier; // Already included

class TransactionManager {
public:
    // Transaction class represents a sequence of operations to be executed atomically.
    class Transaction {
    public:
        // Constructor takes a connection (leased from manager) and potentially PathParser/JSONModifier
        // if paths/values need pre-processing before being added as Redis commands.
        // The Transaction object will use this single connection for MULTI/EXEC.
        explicit Transaction(RedisConnectionManager::RedisConnectionPtr conn,
                             PathParser* path_parser,
                             JSONModifier* json_modifier);
        ~Transaction();

        // Disable copy/move for simplicity, or implement them carefully.
        Transaction(const Transaction&) = delete;
        Transaction& operator=(const Transaction&) = delete;
        // Transaction(Transaction&&) = default; // Default move might be ok if members are suitable
        // Transaction& operator=(Transaction&&) = default;


        // --- Fluent API for adding commands to the transaction ---
        // These methods will queue up the Redis commands (e.g., "SET", "GET", "JSON.SET", "JSON.GET" via Lua)
        // They don't execute immediately.

        // Document operations (simplified, assumes whole doc is a JSON string)
        Transaction& set_json_string(const std::string& key, const std::string& json_string_value);
        Transaction& get_json_string(const std::string& key); // Gets the raw JSON string
        Transaction& del_json_document(const std::string& key); // Deletes the whole key


        // Path operations (these would typically translate to Lua script calls)
        // These are more complex as they might involve client-side JSON processing first
        // to prepare arguments for a Lua script, or use server-side JSON module commands (if allowed).
        // For "no RedisJSON module", these MUST use Lua.

        // Transaction& set_path(const std::string& key, const std::string& path_str, const json& value);
        // Transaction& get_path(const std::string& key, const std::string& path_str);
        // Transaction& del_path(const std::string& key, const std::string& path_str);

        // For simplicity in this step, focusing on raw Redis commands that can be part of MULTI/EXEC.
        // Complex JSON path operations within a transaction would queue EVALSHA commands.

        /**
         * Queues a WATCH command for optimistic locking.
         * Must be called before any command that modifies data.
         * If any watched key is modified by another client before EXEC, the transaction will fail.
         */
        Transaction& watch(const std::string& key);
        Transaction& watch(const std::vector<std::string>& keys);


        /**
         * Executes all queued commands in the transaction (MULTI/EXEC).
         * @return A vector of json objects, where each object is the result of a command.
         *         The structure of each json object depends on what redis_reply_to_json produces for that command's reply.
         *         If the transaction was aborted (e.g., due to WATCH), throws TransactionException.
         *         Redis NIL replies from EXEC (aborted transaction) are handled.
         * @throws TransactionException if EXEC fails or returns NIL (aborted).
         * @throws ConnectionException on connection issues.
         * @throws RedisCommandException for errors within command replies in the transaction.
         */
        std::vector<json> execute();

        /**
         * Discards the transaction (DISCARD).
         * Clears queued commands and releases WATCH locks.
         * Should be called if execute() is not, or if execute() throws and cleanup is needed.
         * Idempotent.
         */
        void discard();

    private:
        friend class TransactionManager; // Allow TransactionManager to construct/return it.

        std::unique_ptr<RedisConnection> connection_; // Single connection for this transaction
        PathParser* path_parser_;     // Non-owning, for path validation/parsing if needed
        JSONModifier* json_modifier_; // Non-owning, for JSON ops if needed client-side

        bool active_ = false; // True after MULTI is sent, false after EXEC/DISCARD
        bool discarded_ = false;
        std::vector<std::tuple<std::string, std::vector<std::string>>> command_queue_; // Command name, args

        // Helper to convert Redis multi-bulk reply (from EXEC) to vector<json>
        std::vector<json> process_exec_reply(redisReply* exec_reply);
        json redis_reply_to_json_transaction(redisReply* reply) const; // Similar to LuaScriptManager's but for general commands

        void send_command_to_redis(const char* cmd, const std::vector<std::string>& args);
        void queue_command(const std::string& cmd_name, std::vector<std::string> cmd_args);
    };


    // --- TransactionManager Public API ---
    explicit TransactionManager(RedisConnectionManager* conn_manager,
                                PathParser* path_parser,
                                JSONModifier* json_modifier);
    ~TransactionManager();

    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;

    /**
     * Begins a new transaction.
     * This will obtain a connection from the RedisConnectionManager.
     * The returned Transaction object is responsible for that connection's lifecycle
     * (i.e., it should be returned to the manager when Transaction is done).
     * @return A unique_ptr to a Transaction object.
     * @throws ConnectionException if a connection cannot be obtained.
     */
    std::unique_ptr<Transaction> begin_transaction();

    /**
     * Enables or disables optimistic locking (WATCH/EXEC semantics) for transactions by default.
     * This is a conceptual setting; individual transactions still control WATCH calls.
     * Might not be directly useful if WATCH is explicit per transaction.
     */
    // void enable_optimistic_locking(bool enabled); // Potentially remove if WATCH is always explicit

private:
    RedisConnectionManager* connection_manager_; // Non-owning
    PathParser* path_parser_;                   // Non-owning
    JSONModifier* json_modifier_;               // Non-owning
    // bool optimistic_locking_default_ = true; // Potentially remove
};

} // namespace redisjson
