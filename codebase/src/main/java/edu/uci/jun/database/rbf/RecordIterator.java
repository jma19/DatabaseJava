package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.rbf.RecordID;
import edu.uci.jun.database.table.Table;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An implementation of Iterator that takes in a RecordID iterator provides iteration over Records
 *
 */
public class RecordIterator implements Iterator<Record> {
  private Iterator<RecordID> recordIDIter;
  private Table table;

  public RecordIterator(Table table, Iterator<RecordID> recIDIter) {
    this.recordIDIter = recIDIter;
    this.table = table;
  }

  public boolean hasNext() {
    return recordIDIter.hasNext();
  }

  public Record next() {
    if (hasNext()) {
      try {
        return table.getRecord(recordIDIter.next());
      } catch (DatabaseException e) {
        throw new NoSuchElementException();
      }
    }
    throw new NoSuchElementException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}

