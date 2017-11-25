package edu.uci.jun.database.rm;

/**
 * Created by junm5 on 11/23/17.
 */
public class ColumnTuple {
    private int tableId;
    private String columnName;
    private String columnType;
    private int columnLength;
    private int columnPosition;
    private boolean isSystemInfo;

    public byte[] getBytes() {
        return null;
    }

}
