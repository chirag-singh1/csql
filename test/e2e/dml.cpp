#include "gtest/gtest.h"
#include "../../lib/rapidjson/document.h"

#include "../../src/analyzer/analyzer.h"
#include "../../src/metadata/metadata_store.h"
#include "../../src/csql.h"

TEST(DML, testInsertValues) {
    Analyzer analyzer;
    MetadataStore m;
    execute_query(&analyzer, &m, "CREATE TABLE tbl (a INT, b STRING, c BOOl)");
    execute_query(&analyzer, &m, "INSERT INTO tbl VALUES (1, 'abc', true)");
}
