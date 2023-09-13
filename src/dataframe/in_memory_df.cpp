#include "in_memory_df.h"
#include "../constants.h"
#include "../util/log.h"
#include "../metadata/metadata_store.h"
#include "../util/file.h"

#include <iostream>
#include <vector>

InMemoryDF::InMemoryDF(std::vector<DataType> col_types) {
    init(col_types, INITIAL_CAPACITY);
}

InMemoryDF::InMemoryDF(Schema* schema) {
    std::vector<DataType> col_types;

    for (auto itr = schema->columns.begin(); itr != schema->columns.end(); itr++) {
        col_types.push_back(itr->second);
    }
    init(col_types, INITIAL_CAPACITY);
}

InMemoryDF::InMemoryDF(std::vector<DataType> col_types, int initial_capacity) {
    init(col_types, initial_capacity);
}

InMemoryDF::InMemoryDF(Schema* schema, int initial_capacity) {
    std::vector<DataType> col_types;

    for (auto itr = schema->columns.begin(); itr != schema->columns.end(); itr++) {
        col_types.push_back(itr->second);
    }
    init(col_types, initial_capacity);
}

InMemoryDF::InMemoryDF(InMemoryDF* original) {
    // Copy column types and initialize data.
    std::vector<DataType> ctypes(original->get_num_columns(), TYPE_INT);
    for (int i = 0; i < get_num_columns(); i++) {
        ctypes[i] = original->get_col_type(i);
    }
    init(ctypes, original->get_capacity());

    // Copy data.
    num_records = original->get_num_records();
    int num_columns = get_num_columns();
    for (int i = 0; i < ctypes.size(); i++) {
        if (ctypes[i] == TYPE_INT) {
            memcpy(int_data[int_cols[i]], original->get_int_data(i), sizeof(int) * num_records);
        }
        else if (ctypes[i] == TYPE_BOOL) {
            memcpy(bool_data[bool_cols[i]], original->get_bool_data(i), sizeof(bool) * num_records);
        }
        else if (ctypes[i] == TYPE_STRING) {
            for (int j = 0; j < num_records; j++) {
                char** orig_data = original->get_str_data(i);
                int length_to_copy = strlen(orig_data[j]) + 1;
                str_data[str_cols[i]][j] = new char[length_to_copy];
                memcpy(str_data[str_cols[i]][j], orig_data[j], sizeof(char) * length_to_copy);
            }
        }
    }

}

void InMemoryDF::init(std::vector<DataType> col_types, int initial_capacity) {
    num_records = 0;
    curr_capacity = initial_capacity;
    num_int = 0;
    num_bool = 0;
    num_str = 0;

    for (int i = 0; i < col_types.size(); i++) {
        if (col_types[i] == TYPE_INT) {
            int_cols[i] = num_int;
            num_int++;
        }
        else if (col_types[i] == TYPE_BOOL) {
            bool_cols[i] = num_bool;
            num_bool++;
        }
        else if (col_types[i] == TYPE_STRING) {
            str_cols[i] = num_str;
            num_str++;
        }
    }

    int_data = new int*[num_int];
    bool_data = new bool*[num_bool];
    str_data = new char**[num_str];
    str_lengths = new int*[num_str];

    for (int i = 0; i < num_int; i++) {
        int_data[i] = new int[initial_capacity];
    }
    for (int i = 0; i < num_bool; i++) {
        bool_data[i] = new bool[initial_capacity];
    }
    for (int i = 0; i < num_str; i++) {
        str_data[i] = new char*[initial_capacity];
        str_lengths[i] = new int[initial_capacity];
    }
}

InMemoryDF::~InMemoryDF() {
    for (int i = 0; i < num_int; i++) {
        delete[] int_data[i];
    }
    if (num_int > 0) delete[] int_data;
    for (int i = 0; i < num_bool; i++) {
        delete[] bool_data[i];
    }
    if (num_bool > 0) delete[] bool_data;
    for (int i = 0; i < num_str; i++) {
        for (int j = 0; j < num_records; j++) {
            delete[] str_data[i][j];
        }
        delete[] str_lengths[i];
        delete[] str_data[i];
    }
    if (num_str > 0) {
        delete[] str_lengths;
        delete[] str_data;
    }
}

bool InMemoryDF::insert_record(Record record) {
    assert(num_records <= curr_capacity); // TODO: expand capacity.

    // Validate insertion schema.
    if (record.num_cols != num_bool + num_int + num_str) {
        LOG_DEBUG("Schema mismatch, wrong number of columns", record.num_cols);
        return false;
    }
    for (int i = 0; i < record.num_cols; i++) {
        if (record.col_types[i] == TYPE_INT && int_cols.find(i) == int_cols.end()) {
            LOG_DEBUG("Schema mismatch, found wrong type column at", i);
            return false;
        }
        if (record.col_types[i] == TYPE_BOOL && bool_cols.find(i) == bool_cols.end()) {
            LOG_DEBUG("Schema mismatch, found wrong type column at", i);
            return false;
        }
        if (record.col_types[i] == TYPE_STRING && str_cols.find(i) == str_cols.end()) {
            LOG_DEBUG("Schema mismatch, found wrong type column at", i);
            return false;
        }
    }

    // Copy values.
    for (int i = 0; i < num_int; i++) {
        int_data[i][num_records] = record.int_vals[i];
    }
    for (int i = 0; i < num_bool; i++) {
        bool_data[i][num_records] = record.bool_vals[i];
    }
    for (int i = 0; i < num_str; i++) {
        str_data[i][num_records] = new char[strlen(record.str_vals[i]) + 1];
        strcpy(str_data[i][num_records], record.str_vals[i]);
    }

    num_records += 1;
    return true;
}

Record* InMemoryDF::get_record() {
    Record* r = new Record;
    r->int_vals = new int[num_int];
    r->bool_vals = new bool[num_int];
    r->str_vals = new char*[num_str];

    r->num_str = num_str;
    r->num_bool = num_bool;
    r->num_int = num_int;
    r->num_cols = num_bool + num_int + num_str;

    r->col_types = new int[r->num_cols];

    for (auto itr = int_cols.begin(); itr != int_cols.end(); itr++) {
        r->col_types[itr->first] = TYPE_INT;
    }
    for (auto itr = bool_cols.begin(); itr != bool_cols.end(); itr++) {
        r->col_types[itr->first] = TYPE_BOOL;
    }
    for (auto itr = str_cols.begin(); itr != str_cols.end(); itr++) {
        r->col_types[itr->first] = TYPE_STRING;
    }

    return r;
}

bool InMemoryDF::to_disk(MetadataStore* m, std::string name) {
    std::string filepath = m->get_table_filepath(name);
    if (filepath == "") {
        LOG_DEBUG("Disk write failed, table not found", name);
        return false;
    }

    check_table_filesystem(filepath);
}

void InMemoryDF::print() {
    int num_cols = num_bool + num_int + num_str;
    size_t* max_lengths = new size_t[num_cols];
    memset(max_lengths, 0, num_cols * sizeof(size_t));
    for (int i = 0; i < num_records; i++) {
        for (auto itr = int_cols.begin(); itr != int_cols.end(); itr++) {
            max_lengths[itr->first] = std::max(max_lengths[itr->first], std::to_string(int_data[itr->second][i]).size());
        }
        for (auto itr = bool_cols.begin(); itr != bool_cols.end(); itr++) {
            max_lengths[itr->first] = std::max(max_lengths[itr->first], strlen(bool_cast(bool_data[itr->second][i])));
        }
        for (auto itr = str_cols.begin(); itr != str_cols.end(); itr++) {
            max_lengths[itr->first] = std::max(max_lengths[itr->first], strlen(str_data[itr->second][i]));
        }
    }
    size_t tot_length = num_cols + 1;
    for (int i = 0; i < num_cols; i++) {
        tot_length += max_lengths[i];
    }
    std::cout << std::string(tot_length, '-') << std::endl;
    std::cout << "|";
    for (int i = 0; i < num_cols; i++) {
        std::cout << i << std::string(max_lengths[i] - std::to_string(i).size(), ' ') << "|";
    }
    std::cout << std::endl;
    std::cout << std::string(tot_length, '-') << std::endl;
    for (int i = 0; i < num_records; i++) {
        std::cout << "|";
        for (int j = 0; j < num_cols; j++) {
            if (int_cols.find(j) != int_cols.end()) {
                int val = int_data[int_cols.find(j)->second][i];
                std::cout << val << std::string(max_lengths[j] - std::to_string(val).size(), ' ') << "|";
            }
            else if (bool_cols.find(j) != bool_cols.end()) {
                bool val = bool_data[bool_cols.find(j)->second][i];
                std::cout << bool_cast(val) << std::string(max_lengths[j] - strlen(bool_cast(val)), ' ') << "|";
            }
            else if (str_cols.find(j) != str_cols.end()) {
                char* val = str_data[str_cols.find(j)->second][i];
                std::cout << val << std::string(max_lengths[j] - strlen(val), ' ') << "|";
            }
        }
        std::cout << std::endl;
        std::cout << std::string(tot_length, '-') << std::endl;
    }

    delete[] max_lengths;
}

int InMemoryDF::get_num_records() {
    return num_records;
}

int InMemoryDF::get_capacity() {
    return curr_capacity;
}

int InMemoryDF::get_num_columns() {
    return num_bool + num_int + num_str;
}

DataType InMemoryDF::get_col_type(int ind) {
    if (int_cols.find(ind) != int_cols.end()) {
        return TYPE_INT;
    }
    else if (bool_cols.find(ind) != bool_cols.end()) {
        return TYPE_BOOL;
    }
    else if (str_cols.find(ind) != str_cols.end()) {
        return TYPE_STRING;
    }
    LOG_DEBUG("Column index not found", ind);
    assert(false); // Must be found!
}

int* InMemoryDF::get_int_data(int col_ind) {
    return int_data[int_cols[col_ind]];
}
char** InMemoryDF::get_str_data(int col_ind) {
    return str_data[str_cols[col_ind]];
}
bool* InMemoryDF::get_bool_data(int col_ind) {
    return bool_data[bool_cols[col_ind]];
}