#pragma once

#include "../../lib/rapidjson/document.h"
#include "../../lib/rapidjson/writer.h"
#include "../../lib/rapidjson/stringbuffer.h"

#include <iostream>
#include <string>

# define LOG_DEBUG(title, text) \
    std::cout << "[DEBUG] " << title << ": " << text << std::endl;

# define LOG_DEBUG_VEC(title, vec) \
    std::cout << "[DEBUG] " << title << ": "; \
    for (int i = 0; i < vec.size(); i++) { \
        std::cout << vec[i]; \
        if (i != vec.size() - 1){ \
            std::cout << ", "; \
        } \
    } \
    std::cout << std::endl;

# define LOG_DEBUG_RAW(text) \
    std::cout << "[DEBUG] " << text << std::endl;

inline void JSON_LOG_DEBUG(std::string title, const rapidjson::Value* val) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    val->Accept(writer);
    std::cout << "[DEBUG] " << title << ": " << buffer.GetString() << std::endl;
}

inline const char* bool_cast(const bool b) {
    return b ? "true" : "false";
}