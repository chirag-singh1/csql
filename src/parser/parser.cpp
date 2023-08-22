#include "parser.h"
#include <cstring>

Parser::Parser(std::string query) {
    char * query_str = new char [query.length()+1];
    std::strcpy (query_str, query.c_str());
    result = pg_query_parse(query_str);
    delete[] query_str;
}

Parser::~Parser() {
    pg_query_free_parse_result(result);
}

PgQueryParseResult* Parser::get_result() {
    return &result;
}