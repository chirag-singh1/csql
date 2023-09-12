#include "metadata_store.h"
#include "../constants.h"
#include "../util/file.h"
#include "../util/log.h"

#include <string>
#include <unordered_map>

MetadataStore::MetadataStore() {
    const rapidjson::Value& metadata
        = read_json_from_file(std::string(DEFAULT_FILEPATH) +
            std::string(METASTORE_FILEPATH));

    // TODO: Load metadata from disk
    databases[DEFAULT_DB_NAME] = new Database(DEFAULT_DB_NAME);
    active_db = DEFAULT_DB_NAME;
}

MetadataStore::~MetadataStore() {
    for (auto itr = databases.begin(); itr != databases.end(); itr++) {
        delete itr->second;
    }
}

bool MetadataStore::create_db(std::string name) {
    LOG_DEBUG("Creating database", name);
    if (db_exists(name)) {
        LOG_DEBUG("Found database", name);
        return false;
    }

    databases[name] = new Database(name);
    LOG_DEBUG("Database created", name);
    return true;
}

bool MetadataStore::drop_db(std::string name, bool missing_ok) {
    LOG_DEBUG("Dropping database", name);
    // Can't drop default DB.
    if (name == DEFAULT_DB_NAME) {
        LOG_DEBUG("Can't delete default DB", name);
        return false;
    }

    if (!db_exists(name)) {
        LOG_DEBUG("No database", name);
        return !missing_ok;
    }

    databases.erase(databases.find(name));
    LOG_DEBUG("Database dropped", name);

    // Reset active DB name if active DB dropped.
    if (active_db == name) {
        active_db = DEFAULT_DB_NAME;
    }

    return true;
}

bool MetadataStore::use_db(std::string name) {
    LOG_DEBUG("Using database", name);
    if(!db_exists(name)) {
        LOG_DEBUG("No database", name);
        return false;
    }

    active_db = name;
    LOG_DEBUG("Using database", name);
    return true;
}

bool MetadataStore::db_exists(std::string name) {
    return databases.find(name) != databases.end();
}

bool MetadataStore::create_table(std::string name, std::vector<std::pair<std::string, std::string>> cols) {
    LOG_DEBUG("Creating table", name);
    Database* curr_db = databases.find(active_db)->second;
    return curr_db->create_table(name, cols);
}