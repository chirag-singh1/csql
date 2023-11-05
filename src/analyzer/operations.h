# pragma once

#include "../../lib/rapidjson/document.h"

#include <string>
#include <unordered_map>

typedef int Operation;

class OperationNode {
    public:
        OperationNode(Operation op, int num_children, std::string name);
        ~OperationNode();
        void set_child(OperationNode* child, int child_ind);
        void set_num_children(int num_children);
        void print(int level);
        int get_num_children();
        OperationNode** get_children();
        Operation get_operation();
        std::string get_string_option(std::string key);
        bool get_bool_option(std::string key);
        int get_int_option(std::string key);
        void set_string_option(std::string key, std::string value);
        void set_bool_option(std::string key, bool value);
        void set_int_option(std::string key, int value);

    private:
        int num_children;
        std::string name;
        Operation op;
        OperationNode** children;
        std::unordered_map<std::string, std::string> string_options;
        std::unordered_map<std::string, bool> bool_options;
        std::unordered_map<std::string, int> int_options;
};


# define NUM_SUPPORTED_OPERATIONS 6
# define OPERATIONS "CreateStmt", "CreatedbStmt", "DropdbStmt", "InsertStmt", \
    "SelectStmt", "DropStmt"

# define OP_UNSUPPORTED -2
# define OP_NONE -1
# define OP_CREATE 0
# define OP_CREATE_DB 1
# define OP_DROP_DB 2
# define OP_INSERT 3
# define OP_SELECT 4
# define OP_DROP_TBL 5

# define OPT_DB_NAME "dbname"
# define OPT_DB_MISSING_OK "missing_ok"
# define OPT_RELATION "relation"
# define OPT_TABLE_NAME "relname"
# define OPT_TABLE_DESC "tableElts"
# define OPT_COL_DEF "ColumnDef"
# define OPT_NUM_COLUMNS "numColumns"
# define OPT_COL_NAMES "colname"
# define OPT_COL_TYPES "typeName"
# define OPT_TYPE_NAMES "names"
# define OPT_COL_NAME(i) "colname" + std::to_string(i)
# define OPT_COL_TYPE(t) "coltype" + std::to_string(t)
# define OPT_STRING "String"
# define OPT_STRING_VAL "sval"
# define OPT_TYPE_NAME_INVALID "pg_catalog"
# define OPT_SELECT_0 "selectStmt"
# define OPT_SELECT_1 "SelectStmt"
# define OPT_VALUE_LIST "valuesLists"
# define OPT_LIST "List"
# define OPT_ITEMS "items"
# define OPT_CONST "A_Const"
# define OPT_INT_VAL "ival"
# define OPT_STR_VAL "sval"
# define OPT_BOOL_VAL "boolval"
# define OPT_CONST_TYPE(i) "const_type" + std::to_string(i)
# define OPT_CONST_VAL(i) "const_val" + std::to_string(i)
# define OPT_SELECT_TARGET "selectTarget"
# define OPT_NUM_VALUES "numValues"
# define OPT_FROM_CLAUSE "fromClause"
# define OPT_RANGE_VAR "RangeVar"
# define OPT_SELECT_TARGETS "targetList"
# define OPT_SELECT_RESTARGET "ResTarget"
# define OPT_SELECT_COLREF "ColumnRef"
# define OPT_SELECT_FIELDS "fields"
# define OPT_SELECT_ALL "A_Star"
# define OPT_VAL "val"
# define OPT_SELECT_NUM_TARGETS "selNumTargets"
# define OPT_SELECT_TARGET_REF(i) "select_target" + std::to_string(i)
# define OPT_OBJECTS "objects"