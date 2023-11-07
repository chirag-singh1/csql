# pragma once

#include "../analyzer/operations.h"
#include "../metadata/schema.h"

#include <string>

# define NUM_SUPPORTED_FILTERS 1
# define FILTERS "="
# define FILTER_EQUALS 0

# define CONST_CONST 0
# define CONST_COL 1
# define COL_COL 2

typedef int ExprType;
typedef int FilterType;

class Table;
class SimpleFilter {
    public:
        SimpleFilter(OperationNode* op, Table* t);

        ExprType get_type();
        FilterType get_predicate_type();
        bool get_const_res();
        bool get_valid();
        int get_const_type();
        int get_int_const();
        bool get_bool_const();
        std::string get_string_const();
        std::pair<int, int> get_col_inds();

    private:
        bool valid;
        ExprType expr;
        FilterType filter;
        bool const_res; // If there is a constant result, no need to evaluate later.
        DataType const_type;
        bool const_bool;
        int const_int;
        std::string const_str;
        int col_ref_left;
        int col_ref_right;
};