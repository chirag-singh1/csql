#pragma once

#include "../metadata/metadata.h"

#include <unordered_map>
#include <vector>

struct Record {
    int* int_vals;
    char** str_vals;
    bool* bool_vals;

    int* col_types;
    int num_cols;
    int num_str;
    int num_int;
    int num_bool;
};

inline void destroy_record(Record* r) {
    delete[] r->int_vals;
    delete[] r->bool_vals;
    delete[] r->col_types;

    for (int i = 0; i < r->num_str; i++) {
        delete[] r->str_vals[i];
    }
    delete[] r->str_vals;

    delete r;
}

class InMemoryDF {
    public:
        InMemoryDF(Schema* schema);
        InMemoryDF(std::vector<DataType> col_types);
        ~InMemoryDF();

        bool insert_record(Record record);
        Record* get_record();

        void print();

    private:
        void init(std::vector<DataType> col_types);

        int** int_data;
        char*** str_data;
        int** str_lengths;
        bool** bool_data;

        int num_int;
        int num_str;
        int num_bool;

        std::unordered_map<int, int> int_cols;
        std::unordered_map<int, int> str_cols;
        std::unordered_map<int, int> bool_cols;

        int num_records;
        int curr_capacity;
};