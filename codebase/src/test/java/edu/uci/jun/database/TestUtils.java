package edu.uci.jun.database;

import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.query.QueryPlanException;
import edu.uci.jun.database.query.TestSourceOperator;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.table.Schema;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {

 public static Schema createSchemaWithAllTypes() {
    List<DataFiled> dataBoxes = new ArrayList<DataFiled>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(new BoolDataFiled());
    dataBoxes.add(new IntDataField());
    dataBoxes.add(new StringDataField(5));
    dataBoxes.add(new FloatDataField());

    fieldNames.add("bool");
    fieldNames.add("int");
    fieldNames.add("string");
    fieldNames.add("float");

    return new Schema(fieldNames, dataBoxes);
  }

 public static Schema createSchemaWithTwoInts() {
    List<DataFiled> dataBoxes = new ArrayList<DataFiled>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(new IntDataField());
    dataBoxes.add(new IntDataField());

    fieldNames.add("int1");
    fieldNames.add("int2");

    return new Schema(fieldNames, dataBoxes);
  }

 public static Schema createSchemaOfBool() {
    List<DataFiled> dataBoxes = new ArrayList<DataFiled>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(new BoolDataFiled());

    fieldNames.add("bool");

    return new Schema(fieldNames, dataBoxes);
  }

 public static Schema createSchemaOfString(int len) {
    List<DataFiled> dataBoxes = new ArrayList<DataFiled>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(new StringDataField(len));
    fieldNames.add("string");

    return new Schema(fieldNames, dataBoxes);
  }


  public static Record createRecordWithAllTypes() {
    List<DataFiled> dataValues = new ArrayList<DataFiled>();
    dataValues.add(new BoolDataFiled(true));
    dataValues.add(new IntDataField(1));
    dataValues.add(new StringDataField("abcde", 5));
    dataValues.add(new FloatDataField((float) 1.2));

    return new Record(dataValues);
  }

  public static Record createRecordWithAllTypesWithValue(int val) {
    List<DataFiled> dataValues = new ArrayList<DataFiled>();
    dataValues.add(new BoolDataFiled(true));
    dataValues.add(new IntDataField(val));
    dataValues.add(new StringDataField(String.format("%05d", val), 5));
    dataValues.add(new FloatDataField((float) val));
    return new Record(dataValues);
  }


  public static TestSourceOperator createTestSourceOperatorWithInts(List<Integer> values)
      throws QueryPlanException {
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    List<DataFiled> columnTypes = new ArrayList<DataFiled>();
    columnTypes.add(new IntDataField());
    Schema schema = new Schema(columnNames, columnTypes);

    List<Record> recordList = new ArrayList<Record>();

    for (int v : values) {
      List<DataFiled> recordValues = new ArrayList<DataFiled>();
      recordValues.add(new IntDataField(v));
      recordList.add(new Record(recordValues));
    }


    return new TestSourceOperator(recordList, schema);
  }

  public static TestSourceOperator createTestSourceOperatorWithFloats(List<Float> values)
      throws QueryPlanException {
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("float");
    List<DataFiled> columnTypes = new ArrayList<DataFiled>();
    columnTypes.add(new FloatDataField());
    Schema schema = new Schema(columnNames, columnTypes);

    List<Record> recordList = new ArrayList<Record>();

    for (float v : values) {
      List<DataFiled> recordValues = new ArrayList<DataFiled>();
      recordValues.add(new FloatDataField(v));
      recordList.add(new Record(recordValues));
    }


    return new TestSourceOperator(recordList, schema);
  }
}
