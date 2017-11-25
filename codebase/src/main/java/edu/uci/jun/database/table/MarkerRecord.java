package edu.uci.jun.database.table;

import java.util.ArrayList;

import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.rbf.Record;

/**
 * An empty record used to delineate groups in the GroupByOperator.
 */
public class MarkerRecord extends Record {
  private static final MarkerRecord record = new MarkerRecord();

  private MarkerRecord() {
    super(new ArrayList<DataFiled>());
  }

  public static MarkerRecord getMarker() {
    return MarkerRecord.record;
  }
}
