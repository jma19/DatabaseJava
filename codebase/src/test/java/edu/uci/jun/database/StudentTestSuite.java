package edu.uci.jun.database;

import edu.uci.jun.database.datafiled.TestBoolDataBox;
import edu.uci.jun.database.datafiled.TestFloatDataBox;
import edu.uci.jun.database.datafiled.TestIntDataBox;
import edu.uci.jun.database.datafiled.TestStringDataBox;
import edu.uci.jun.database.io.TestLRUCache;
import edu.uci.jun.database.io.TestPage;
import edu.uci.jun.database.io.TestPageAllocator;
import edu.uci.jun.database.query.*;
import edu.uci.jun.database.table.TestTable;
import edu.uci.jun.database.table.stats.*;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runners.Suite.SuiteClasses;

import edu.uci.jun.database.table.TestSchema;

/**
 * A test suite for student tests.
 *
 * DO NOT CHANGE ANY OF THIS CODE.
 */
@RunWith(Categories.class)
@IncludeCategory(StudentTest.class)
@SuiteClasses({
    TestBoolDataBox.class,
    TestFloatDataBox.class,
        TestIntDataBox.class,
        TestStringDataBox.class,
        TestLRUCache.class,
        TestPage.class,
        TestPageAllocator.class,
        GroupByOperatorTest.class,
        JoinOperatorTest.class,
        QueryPlanTest.class,
        SelectOperatorTest.class,
        WhereOperatorTest.class,
        BoolHistogramTest.class,
        FloatHistogramTest.class,
        IntHistogramTest.class,
        StringHistogramTest.class,
        TableStatsTest.class,
        TestSchema.class,
        TestTable.class,
        TestDatabase.class,
        TestDatabaseQueries.class
})
public class StudentTestSuite {}
