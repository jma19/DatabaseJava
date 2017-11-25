package edu.uci.jun.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.table.Schema;

public class JoinOperator extends QueryOperator {
  private QueryOperator leftSource;
  private QueryOperator rightSource;
  private int leftColumnIndex;
  private int rightColumnIndex;
  private String leftColumnName;
  private String rightColumnName;

  /**
   * Create a join operator that pulls tuples from leftSource and rightSource. Returns tuples for which
   * leftColumnName and rightColumnName are equal.
   *
   * @param leftSource the left source operator
   * @param rightSource the right source operator
   * @param leftColumnName the column to join on from leftSource
   * @param rightColumnName the column to join on from rightSource
   * @throws QueryPlanException
   */
  public JoinOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName) throws QueryPlanException {
    super(OperatorType.JOIN);

    this.leftSource = leftSource;
    this.rightSource = rightSource;

    this.leftColumnName = leftColumnName;
    this.rightColumnName = rightColumnName;
    this.setOutputSchema(this.computeSchema());
  }

  /**
   * Joins tuples from leftSource and rightSource and returns an iterator of records.
   *
   * @return an iterator of records
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public Iterator<Record> execute() throws QueryPlanException, DatabaseException {
    List<Record> newRecords = new ArrayList<Record>();
    Iterator<Record> leftIterator = this.leftSource.execute();

    while (leftIterator.hasNext()) {
      Record leftRecord = leftIterator.next();

      Iterator<Record> rightIterator = this.rightSource.execute();
      while (rightIterator.hasNext()) {
        Record rightRecord = rightIterator.next();

        DataFiled leftJoinValue = leftRecord.getValues().get(this.leftColumnIndex);
        DataFiled rightJoinValue = rightRecord.getValues().get(this.rightColumnIndex);

        if (leftJoinValue.equals(rightJoinValue)) {
          List<DataFiled> leftValues = new ArrayList<DataFiled>(leftRecord.getValues());
          List<DataFiled> rightValues = new ArrayList<DataFiled>(rightRecord.getValues());

          leftValues.addAll(rightValues);
          newRecords.add(new Record(leftValues));
        }
      }
    }

    return newRecords.iterator();
  }

  @Override
  public QueryOperator getSource() throws QueryPlanException {
    throw new QueryPlanException("There is no single source for join operators. Please use " +
        "getRightSource and getLeftSource and the corresponding set methods.");
  }

  public QueryOperator getLeftSource() {
    return this.leftSource;
  }

  public QueryOperator getRightSource() {
    return this.rightSource;
  }

  public void setLeftSource(QueryOperator leftSource) {
    this.leftSource = leftSource;
  }

  public void setRightSource(QueryOperator rightSource) {
    this.rightSource = rightSource;
  }

  public Schema computeSchema() throws QueryPlanException {
    Schema leftSchema = this.leftSource.getOutputSchema();
    Schema rightSchema = this.rightSource.getOutputSchema();
    List<String> leftSchemaNames = new ArrayList<String>(leftSchema.getFieldNames());
    List<String> rightSchemaNames = new ArrayList<String>(rightSchema.getFieldNames());

    this.leftColumnName = this.checkSchemaForColumn(leftSchema, this.leftColumnName);
    this.leftColumnIndex = leftSchemaNames.indexOf(leftColumnName);

    this.rightColumnName = this.checkSchemaForColumn(rightSchema, this.rightColumnName);
    this.rightColumnIndex = rightSchemaNames.indexOf(rightColumnName);

    List<DataFiled> leftSchemaTypes = new ArrayList<DataFiled>(leftSchema.getFieldTypes());
    List<DataFiled> rightSchemaTypes = new ArrayList<DataFiled>(rightSchema.getFieldTypes());

    if (!leftSchemaTypes.get(this.leftColumnIndex).getClass().equals(rightSchemaTypes.get(
        this.rightColumnIndex).getClass())) {
      throw new QueryPlanException("Mismatched types of columns " + leftColumnName + " and "
          + rightColumnName + ".");
    }

    leftSchemaNames.addAll(rightSchemaNames);
    leftSchemaTypes.addAll(rightSchemaTypes);

    return new Schema(leftSchemaNames, leftSchemaTypes);
  }
}
