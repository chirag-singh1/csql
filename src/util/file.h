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
#include <dirent.h>

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

# define read_json_from_file(_filepath) \
    char* _filepath_c = new char[_filepath.length() + 1]; \
    std::strcpy (_filepath_c, _filepath.c_str()); \
    rapidjson::Document _d; \
    _d.SetObject(); \
    rapidjson::Value& _out = _d.GetObject(); \
    if (FILE *_f = fopen(_filepath_c, "rb")) { \
        fseek(_f, 0, SEEK_END); \
        long _fsize = ftell(_f); \
        fseek(_f, 0, SEEK_SET); \
                                \
        char *_file_in = new char[_fsize + 1]; \
        fread(_file_in, _fsize, 1, _f); \
        fclose(_f); \
 \
        _file_in[_fsize] = 0; \
 \
        LOG_DEBUG("Found JSON", _file_in); \
        rapidjson::Document _d; \
        rapidjson::Value& _f_out = _d.Parse(_file_in); \
        JSON_LOG_DEBUG("Parsed JSON", &_f_out); \
        delete[] _file_in; \
        _out = _f_out.GetObject(); \
    } \
    delete[] _filepath_c;

// This is VERY insecure, but it's OK (i don't care).
inline void create_path(char* path) {
    LOG_DEBUG("Creating path", path);
    char* command = new char[strlen(DEFAULT_FILEPATH) + 1 + strlen("mkdir -p ")];
    strcpy(command, "mkdir -p ");
    strcpy(command + strlen("mkdir -p "), DEFAULT_FILEPATH);
    int ret = system(command);
    LOG_DEBUG("Path creation returned", ret);
    delete[] command;
}

inline void init_filesystem() {
    // If the metadata file isn't found, create the datapath and metadata file.
    struct stat buffer;
    if (stat(metastore_filepath().c_str(), &buffer) != 0) {
        LOG_DEBUG("Data folder not found", metastore_filepath());
        create_path(DEFAULT_FILEPATH);

        rapidjson::Document d;
        rapidjson::Value v;
        v.SetObject();
        d.SetObject();
        d.AddMember(DEFAULT_DB_NAME, v, d.GetAllocator());
        write_json_to_file(&d, metastore_filepath());
    }
}

inline void check_table_filesystem(std::string filepath) {
    char* path = new char[filepath.size() + 1];
    strcpy(path, filepath.c_str());

    // If the path doesn't exist create it, otherwise delete all data files.
    struct stat buffer;
    if (stat(path, &buffer) != 0) {
        create_path(path);
    }
    else {
        struct dirent *ent;
        DIR *dir = opendir(path);
        while ((ent = readdir(dir)) != NULL) {
            std::remove((filepath + ent->d_name).c_str());
        }
        closedir (dir);
    }
}