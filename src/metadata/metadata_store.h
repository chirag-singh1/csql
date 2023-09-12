#pragma once

#include "../../lib/rapidjson/document.h"
#include "metadata.h"

#include <unordered_map>
#include <string>
#include <vector>

class MetadataStore {
    public:
        MetadataStore();
        ~MetadataStore();
        bool create_db(std::string name);
        bool drop_db(std::string name, bool missing_ok);
        bool use_db(std::string name);
        bool create_table(std::string name, std::vector<std::pair<std::string, std::string>> cols);
        bool db_exists(std::string name);

    private:
        std::unordered_map<std::string, Database*> databases;
        std::string active_db;
};