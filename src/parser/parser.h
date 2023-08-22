#pragma once

#include "pg_query.h"
#include <string>

class Parser {
    public:
        Parser(std::string query);
        ~Parser();
        PgQueryParseResult* get_result();

    private:
        PgQueryParseResult result;
};