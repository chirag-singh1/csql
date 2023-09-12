#pragma once

#include "../../lib/rapidjson/document.h"
#include "../../lib/rapidjson/writer.h"
#include "../../lib/rapidjson/stringbuffer.h"

#include <iostream>
#include <string>

# define LOG_DEBUG(title, text) \
    std::cout << "[DEBUG] " << title << ": " << text << std::endl;

inline void JSON_LOG_DEBUG(std::string title, const rapidjson::Value* val) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    val->Accept(writer);
    std::cout << "[DEBUG] " << title << ": " << buffer.GetString() << std::endl;
}

inline const char* bool_cast(const bool b) {
    return b ? "true" : "false";
}