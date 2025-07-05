#pragma once

#include "common_types.h"
#include <string>
#include <vector>
#include <functional>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>

namespace redisjson {

using json = nlohmann::json;

class JSONEventEmitter {
public:
    enum class EventType {
        CREATED, // Document created
        UPDATED, // Document or path updated
        DELETED, // Document or path deleted
        ACCESSED // Document or path accessed (potentially noisy)
    };

    using EventCallback = std::function<void(EventType type, const std::string& key, const std::optional<std::string>& path, const std::optional<json>& data)>;

    JSONEventEmitter();

    // Registers a callback for a specific event type.
    // Returns a handle or ID that can be used to unregister the callback.
    size_t on_event(EventType type, EventCallback callback);

    // Unregisters a callback using its handle.
    void off_event(EventType type, size_t callback_handle);

    // Emits an event to all registered listeners for that event type.
    // This would typically be called by RedisJSONClient after relevant operations.
    void emit_event(EventType type, const std::string& key, const std::optional<std::string>& path = std::nullopt, const std::optional<json>& data = std::nullopt);

    // Enables or disables event emission globally.
    void enable_events(bool enabled);
    bool are_events_enabled() const;

private:
    struct CallbackInfo {
        size_t id;
        EventCallback callback;
    };

    std::map<EventType, std::vector<CallbackInfo>> _listeners;
    mutable std::mutex _mutex;
    bool _events_enabled = true;
    size_t _next_callback_id = 1;
};

} // namespace redisjson
