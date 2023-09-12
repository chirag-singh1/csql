# pragma once

#include "../constants.h"
#include "log.h"
#include "../../lib/rapidjson/document.h"
#include "../../lib/rapidjson/writer.h"
#include "../../lib/rapidjson/stringbuffer.h"

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <sys/stat.h>

inline std::string metastore_filepath() {
    return std::string(DEFAULT_FILEPATH) + std::string(METASTORE_FILEPATH);
}

inline void write_json_to_file(const rapidjson::Value* json,
    std::string filepath) {
    // Convert JSON to string.
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json->Accept(writer);

    // Write JSON out to file.
    std::ofstream output_file(filepath);
    output_file << buffer.GetString();
    output_file.close();
}

inline const rapidjson::Value& read_json_from_file(std::string filepath) {
    // Read from file into char*.
    char* filepath_c = new char[filepath.length() + 1];
    std::strcpy (filepath_c, filepath.c_str());
    if (FILE *f = fopen(filepath_c, "rb")) {
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);

        char *file_in = new char[fsize + 1];
        fread(file_in, fsize, 1, f);
        fclose(f);

        file_in[fsize] = 0;

        rapidjson::Document d;
        const rapidjson::Value& out = d.Parse(file_in);
        delete[] file_in;
        delete[] filepath_c;
        return out;
    }

    delete[] filepath_c;
    rapidjson::Document d;
    d.SetObject();
    return d;
}

inline void init_filesystem() {
    // If the metadata file isn't found, create the datapath and metadata file.
    struct stat buffer;
    if (stat(metastore_filepath().c_str(), &buffer) != 0) {
        LOG_DEBUG("Data folder not found", metastore_filepath());
        char* command = new char[strlen(DEFAULT_FILEPATH) + 1 + strlen("mkdir -p ")];
        strcpy(command, "mkdir -p ");
        strcpy(command + strlen("mkdir -p "), DEFAULT_FILEPATH);
        system(command);
        delete[] command;

        rapidjson::Document d;
        rapidjson::Value v;
        v.SetObject();
        d.SetObject();
        d.AddMember(DEFAULT_DB_NAME, v, d.GetAllocator());
        write_json_to_file(&d, metastore_filepath());
    }
}