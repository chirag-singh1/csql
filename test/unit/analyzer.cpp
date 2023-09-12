#include "gtest/gtest.h"
#include "../../lib/rapidjson/document.h"

#include "../../src/analyzer/operations.h"
#include "../../src/analyzer/analyzer.h"
#include "../../src/util/log.h"
#include "../../src/parser/parser.h"

TEST(Analyzer, testParseCreateDatabase) {
    Parser p = Parser("CREATE DATABASE db");
    Analyzer a;
    OperationNode* op = a.query_to_node(p.get_result()->parse_tree);
    EXPECT_EQ (op->get_num_children(), 1);
    EXPECT_EQ (op->get_operation(), OP_NONE);
    op = op->get_children()[0];
    EXPECT_EQ (op->get_num_children(), 0);
    EXPECT_EQ (op->get_operation(), OP_CREATE_DB);
}