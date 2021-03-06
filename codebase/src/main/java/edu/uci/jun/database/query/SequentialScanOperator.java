package edu.uci.jun.database.query;

import java.util.Iterator;

import edu.uci.jun.database.Database;
import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.table.Schema;

public class SequentialScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;

  /**
   * Creates a new SequentialScanOperator that provides an iterator on all tuples in a table.
   *
   * NOTE: Sequential scans don't take a source operator because they must always be at the bottom
   * of the DAG.
   *
   * @param transaction
   * @param tableName
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public SequentialScanOperator(Database.Transaction transaction,
                                String tableName) throws QueryPlanException, DatabaseException {
    super(OperatorType.SEQSCAN);

    this.transaction = transaction;
    this.tableName = tableName;

    this.setOutputSchema(this.computeSchema());
  }

  public Iterator<Record> execute() throws DatabaseException {
    return this.transaction.getRecordIterator(tableName);
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }
}
