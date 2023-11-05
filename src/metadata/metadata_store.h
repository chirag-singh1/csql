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
        bool drop_table(std::string name);
        Table* get_table(std::string name);
        bool db_exists(std::string name);
        std::string get_table_filepath(std::string name);

    private:
        void persist_metadata();
        std::unordered_map<std::string, Database*> databases;
        std::string active_db;
        bool skip_persist_metadata;
};