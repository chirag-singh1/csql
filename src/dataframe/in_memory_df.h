#pragma once

#include "../metadata/schema.h"
#include "../analyzer/operations.h"

#include <unordered_map>
#include <vector>

class SimpleFilter;
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
        InMemoryDF(InMemoryDF* original, bool schema_only);
        InMemoryDF(InMemoryDF* original, std::vector<int> projected_cols);
        ~InMemoryDF();
        InMemoryDF* deep_clone();
        InMemoryDF* simple_filter(SimpleFilter* op);

        bool insert_record(Record record);
        bool merge_df(InMemoryDF* df);
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

# define COPY_RECORD(dst_df, src_ind, dst_ind) \
    for (int _i = 0; _i < num_int; _i++) {  \
        dst_df->int_data[_i][dst_ind] = int_data[_i][src_ind]; \
    } \
    for (int _i = 0; _i < num_int; _i++) { \
        dst_df->bool_data[_i][dst_ind] = bool_data[_i][src_ind]; \
    } \
    for (int _i = 0; _i < num_int; _i++) { \
        dst_df->str_data[_i][dst_ind] = new char[strlen(str_data[_i][src_ind]) + 1]; \
        strcpy(dst_df->str_data[_i][dst_ind], str_data[_i][src_ind]); \
    } \
    dst_ind++;
