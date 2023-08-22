#include "metadata_store.h"
#include "../constants.h"
#include "../util/file.h"

MetadataStore::MetadataStore() {
    const rapidjson::Value& in_memory_metadata = read_json_from_file(DEFAULT_FILEPATH);
}
