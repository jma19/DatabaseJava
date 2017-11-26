package edu.uci.jun.database.query;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.uci.jun.database.Database;
import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.TestUtils;
import edu.uci.jun.database.table.MarkerRecord;
import edu.uci.jun.database.rbf.Record;

import static org.junit.Assert.*;

public class GroupByOperatorTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testOperatorSchema() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    GroupByOperator groupByOperator = new GroupByOperator(sourceOperator, null, "int");

    assertEquals(TestUtils.createSchemaWithAllTypes(), groupByOperator.getOutputSchema());
  }

  @Test
  public void testSimpleGroupBy() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("groupByTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    List<Integer> values = new ArrayList<Integer>();
    Set<Integer> addedValues = new HashSet<Integer>();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 100; j++) {
        values.add(j);
        addedValues.add(j);
      }
    }

    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    GroupByOperator groupByOperator = new GroupByOperator(sourceOperator, transaction, "int");
    Iterator<Record> outputIterator = groupByOperator.execute();
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    int count = 0;
    int valueCount = 0;
    int prevValue = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();
      if (record == markerRecord) {
        addedValues.remove(prevValue);
        assertEquals(4, valueCount);

        valueCount = 0;
      } else {
        int recordValue = record.getValues().get(0).getInt();
        assertTrue(addedValues.contains(recordValue));
        prevValue = recordValue;
        valueCount++;

        count++;
      }
    }

    // make sure to remove the last one because there is no MarkerRecord
    assertEquals(4, valueCount);
    addedValues.remove(prevValue);

    assertEquals(400, count);
    assertEquals(0, addedValues.size());
  }

  @Test
  public void testEmptyGroupBy() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("groupByTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    List<Integer> values = new ArrayList<Integer>();

    TestSourceOperator sourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    GroupByOperator groupByOperator = new GroupByOperator(sourceOperator, transaction, "int");
    Iterator<Record> outputIterator = groupByOperator.execute();

    assertFalse(outputIterator.hasNext());
  }

  @Test(expected = QueryPlanException.class)
  public void testGroupByNonexistentColumn() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();

    new GroupByOperator(sourceOperator, null, "nonexistentColumn");
  }
}
