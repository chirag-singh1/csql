#pragma once

#include "../metadata/schema.h"

#include <unordered_map>
#include <vector>

class MetadataStore;

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
        InMemoryDF(Schema* schema, int initial_capacity);
        InMemoryDF(std::vector<DataType> col_types, int initial_capacity);
        InMemoryDF(InMemoryDF* original);
        ~InMemoryDF();
        InMemoryDF* deep_clone();

        bool insert_record(Record record);
        Record* get_record();
        int get_num_records();
        bool to_disk(MetadataStore* m, std::string table_name);
        bool from_disk(MetadataStore* m, std::string table_name);
        int get_capacity();
        int get_num_columns();
        DataType get_col_type(int ind);

        int* get_int_data(int col_ind);
        char** get_str_data(int col_ind);
        bool* get_bool_data(int col_ind);

        void print();

    private:
        InMemoryDF();
        void init(std::vector<DataType> col_types, int initial_capacity);

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
        int version;
};