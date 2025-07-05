#pragma once

#include <hiredis/hiredis.h>
#include <memory> // For std::unique_ptr

namespace redisjson {

// Custom deleter for redisReply
struct RedisReplyDeleter {
    void operator()(redisReply* r) const {
        if (r) {
            freeReplyObject(r);
        }
    }
};

using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

} // namespace redisjson
