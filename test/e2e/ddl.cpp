#include "gtest/gtest.h"
#include "../../lib/rapidjson/document.h"

#include "../../src/analyzer/analyzer.h"
#include "../../src/metadata/metadata_store.h"
#include "../../src/csql.h"

TEST(DDL, testCreateDatabase) {
    Analyzer analyzer;
    MetadataStore m;
    execute_query(&analyzer, &m, "DROP DATABASE IF EXISTS test_db");
    EXPECT_FALSE (m.db_exists("test_db"));
    execute_query(&analyzer, &m, "CREATE DATABASE test_db");
    EXPECT_TRUE (m.db_exists("test_db"));
}

TEST(DDL, testDropDatabase) {
    Analyzer analyzer;
    MetadataStore m;
    execute_query(&analyzer, &m, "DROP DATABASE IF EXISTS test_db");
    EXPECT_FALSE (m.db_exists("test_db"));
    execute_query(&analyzer, &m, "CREATE DATABASE test_db");
    EXPECT_TRUE (m.db_exists("test_db"));
    execute_query(&analyzer, &m, "DROP DATABASE IF EXISTS test_db");
    EXPECT_FALSE (m.db_exists("test_db"));
}

TEST(DDL, testCreateTable) {
    Analyzer analyzer;
    MetadataStore m;
    execute_query(&analyzer, &m, "DROP TABLE IF EXISTS tbl");
    execute_query(&analyzer, &m, "CREATE TABLE tbl (a INT, b STRING, c BOOl)");
}