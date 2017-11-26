package edu.uci.jun.database.query;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.TestUtils;
import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.table.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.jun.database.datafiled.DataFiled;

import static org.junit.Assert.*;

public class JoinOperatorTest {

  @Test
  public void testOperatorSchema() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    JoinOperator joinOperator = new JoinOperator(sourceOperator, sourceOperator, "int", "int");

    List<String> expectedSchemaNames = new ArrayList<String>();
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");

    List<DataFiled> expectedSchemaTypes = new ArrayList<DataFiled>();
    expectedSchemaTypes.add(new BoolDataFiled());
    expectedSchemaTypes.add(new IntDataField());
    expectedSchemaTypes.add(new StringDataField(5));
    expectedSchemaTypes.add(new FloatDataField());
    expectedSchemaTypes.add(new BoolDataFiled());
    expectedSchemaTypes.add(new IntDataField());
    expectedSchemaTypes.add(new StringDataField(5));
    expectedSchemaTypes.add(new FloatDataField());

    Schema expectedSchema = new Schema(expectedSchemaNames, expectedSchemaTypes);

    assertEquals(expectedSchema, joinOperator.getOutputSchema());
  }


  @Test
  public void testSimpleJoin() throws QueryPlanException, DatabaseException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    JoinOperator joinOperator = new JoinOperator(sourceOperator, sourceOperator, "int", "int");

    Iterator<Record> outputIterator = joinOperator.execute();
    int numRecords = 0;
    List<DataFiled> expectedRecordValues = new ArrayList<DataFiled>();
    expectedRecordValues.add(new BoolDataFiled(true));
    expectedRecordValues.add(new IntDataField(1));
    expectedRecordValues.add(new StringDataField("abcde", 5));
    expectedRecordValues.add(new FloatDataField(1.2f));
    expectedRecordValues.add(new BoolDataFiled(true));
    expectedRecordValues.add(new IntDataField(1));
    expectedRecordValues.add(new StringDataField("abcde", 5));
    expectedRecordValues.add(new FloatDataField(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test
  public void testEmptyJoin() throws QueryPlanException, DatabaseException {
    TestSourceOperator leftSourceOperator = new TestSourceOperator();

    List<Integer> values = new ArrayList<Integer>();
    TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);

    JoinOperator joinOperator = new JoinOperator(leftSourceOperator, rightSourceOperator, "int", "int");
    Iterator<Record> outputIterator = joinOperator.execute();

    assertFalse(outputIterator.hasNext());
  }

  @Test(expected = QueryPlanException.class)
  public void testJoinOnInvalidColumn() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();

    new JoinOperator(sourceOperator, sourceOperator, "notAColumn", "int");
  }

  @Test(expected = QueryPlanException.class)
  public void testJoinOnNonMatchingColumn() throws QueryPlanException {
    TestSourceOperator sourceOperator = new TestSourceOperator();

    new JoinOperator(sourceOperator, sourceOperator, "string", "int");
  }
}
