#pragma once

#include "../util/log.h"
#include "../../lib/rapidjson/document.h"

#include <string>
#include <unordered_map>
#include <map>
#include <vector>

class Table;
class MetadataStore;

class Database {
    public:
        Database(std::string name);
        ~Database();
        bool table_exists(std::string name);
        void attach_table(std::string name, Table* t);
        bool create_table(MetadataStore* m, std::string name, std::vector<std::pair<std::string, std::string>> cols);
        Table* get_table(std::string name);
        std::unordered_map<std::string, Table*>* get_tables();

    private:
        std::string name;
        std::unordered_map<std::string, Table*> tables;
};