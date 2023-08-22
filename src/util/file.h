# pragma once

#include "../../lib/rapidjson/document.h"
#include "../../lib/rapidjson/writer.h"
#include "../../lib/rapidjson/stringbuffer.h"

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>

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