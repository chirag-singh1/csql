#include "filter.h"
#include "../table/table.h"

bool check_constant_equality(OperationNode* op) {
    // Types match, compare the types individually.
    if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) == op->get_int_option(OPT_FILTER_EXPR_TYPE(0, false))) {
        if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) == TYPE_INT) {
            return op->get_int_option(OPT_FILTER_EXPR_VAL(0, true)) == op->get_int_option(OPT_FILTER_EXPR_VAL(0, false));
        }
        else if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) == TYPE_BOOL) {
            return op->get_bool_option(OPT_FILTER_EXPR_VAL(0, true)) == op->get_bool_option(OPT_FILTER_EXPR_VAL(0, false));
        }
        else if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) == TYPE_STRING) {
            return op->get_string_option(OPT_FILTER_EXPR_VAL(0, true)) == op->get_string_option(OPT_FILTER_EXPR_VAL(0, false));
        }
        assert(false); // Different data type from supported here.
    }
    return false;
}

SimpleFilter::SimpleFilter(OperationNode* op, Table* t) {
    LOG_DEBUG_RAW("Creating filter");
    valid = true;
    filter = op->get_int_option(OPT_FILTER_EXPR(0));
    // Case 1: lexpr and rexpr are both constant values. Either return a copy of
    // the current DF or an empty DF (with the same schema).
    if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) != TYPE_COL_REF && op->get_int_option(OPT_FILTER_EXPR_TYPE(0, false)) != TYPE_COL_REF) {
        LOG_DEBUG_RAW("Found constant predicate");
        const_res = check_constant_equality(op);
        expr = CONST_CONST;
    }
    // Case 2: One of lexpr and rexpr is a colref, the other is a constant value.
    else if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) != TYPE_COL_REF || op->get_int_option(OPT_FILTER_EXPR_TYPE(0, false)) != TYPE_COL_REF) {
        LOG_DEBUG_RAW("Found constant compared to column");
        expr = CONST_COL;
        bool const_on_left = (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, true)) != TYPE_COL_REF);
        if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, const_on_left)) == TYPE_BOOL) {
            const_type = TYPE_BOOL;
            const_bool = op->get_bool_option(OPT_FILTER_EXPR_VAL(0, const_on_left));
        }
        else if (op->get_int_option(OPT_FILTER_EXPR_TYPE(0, const_on_left)) == TYPE_INT) {
            const_type = TYPE_INT;
            const_int = op->get_int_option(OPT_FILTER_EXPR_VAL(0, const_on_left));
        }
        else {
            const_type = TYPE_STRING;
            const_str = op->get_string_option(OPT_FILTER_EXPR_VAL(0, const_on_left));
        }
        col_ref_left = t->get_col_ind(op->get_string_option(OPT_FILTER_EXPR_VAL(0, !const_on_left)));
        col_ref_right = -1;
        valid = col_ref_left != -1;
    }
    else {
        expr = COL_COL;
        col_ref_left = t->get_col_ind(op->get_string_option(OPT_FILTER_EXPR_VAL(0, true)));
        col_ref_right = t->get_col_ind(op->get_string_option(OPT_FILTER_EXPR_VAL(0, false)));
        valid = (col_ref_left != -1) && (col_ref_right != -1);
    }
}

ExprType SimpleFilter::get_type() {
    return expr;
}

bool SimpleFilter::get_const_res() {
    return const_res;
}

bool SimpleFilter::get_valid() {
    return valid;
}

int SimpleFilter::get_const_type() {
    return const_type;
}

int SimpleFilter::get_int_const() {
    return const_int;
}

bool SimpleFilter::get_bool_const() {
    return const_bool;
}

std::string SimpleFilter::get_string_const() {
    return const_str;
}

std::pair<int, int> SimpleFilter::get_col_inds() {
    return std::make_pair(col_ref_left, col_ref_right);
}

FilterType SimpleFilter::get_predicate_type() {
    return filter;
}