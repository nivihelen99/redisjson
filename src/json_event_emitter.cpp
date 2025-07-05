#include "redisjson++/json_event_emitter.h"
#include <algorithm> // For std::remove_if

namespace redisjson {

JSONEventEmitter::JSONEventEmitter() : _events_enabled(true), _next_callback_id(1) {}

size_t JSONEventEmitter::on_event(EventType type, EventCallback callback) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!callback) {
        // Or throw an exception for null callback
        return 0;
    }
    size_t id = _next_callback_id++;
    _listeners[type].push_back({id, std::move(callback)});
    return id;
}

void JSONEventEmitter::off_event(EventType type, size_t callback_handle) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (callback_handle == 0) return; // Invalid handle

    auto it = _listeners.find(type);
    if (it != _listeners.end()) {
        auto& callbacks = it->second;
        callbacks.erase(
            std::remove_if(callbacks.begin(), callbacks.end(),
                           [callback_handle](const CallbackInfo& info){
                               return info.id == callback_handle;
                           }),
            callbacks.end()
        );
        if (callbacks.empty()) {
            _listeners.erase(it);
        }
    }
}

void JSONEventEmitter::emit_event(EventType type, const std::string& key, const std::optional<std::string>& path, const std::optional<json>& data) {
    // Make a copy of listeners for this event type to avoid issues if a callback
    // tries to modify the listeners map (e.g., unregister itself).
    // This also allows callbacks to run without holding the primary mutex for too long.
    std::vector<CallbackInfo> current_listeners;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_events_enabled) {
            return;
        }
        auto it = _listeners.find(type);
        if (it != _listeners.end()) {
            current_listeners = it->second; // Copy the list of callbacks
        }
    } // Release mutex before invoking callbacks

    if (!current_listeners.empty()) {
        for (const auto& listener_info : current_listeners) {
            try {
                if (listener_info.callback) {
                    listener_info.callback(type, key, path, data);
                }
            } catch (const std::exception& e) {
                // TODO: Add logging for callback exceptions
                // For example: REDISJSON_LOG_ERROR("Exception in event callback: " + std::string(e.what()));
            } catch (...) {
                // TODO: Add logging for unknown exceptions
                // For example: REDISJSON_LOG_ERROR("Unknown exception in event callback.");
            }
        }
    }
}

void JSONEventEmitter::enable_events(bool enabled) {
    std::lock_guard<std::mutex> lock(_mutex);
    _events_enabled = enabled;
}

bool JSONEventEmitter::are_events_enabled() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _events_enabled;
}

} // namespace redisjson
