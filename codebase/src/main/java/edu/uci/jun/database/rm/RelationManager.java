package edu.uci.jun.database.rm;

import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.rbf.RecordBasedFileManager;
import edu.uci.jun.database.rbf.RecordID;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by junm5 on 11/22/17.
 */
public class RelationManager {
    private final static RelationManager instance = new RelationManager();

    public static RelationManager getInstance() {
        return instance;
    }

    //file for storing table information
    private final static String TABLES_TABLE = "tables.tab";
    //file for storing columns of a table
    private final static String COLUMNS_TABLE = "columns.tab";


    public void createCatalog() {
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();

        if (!recordBasedFileManager.isExist(TABLES_TABLE)) {
            recordBasedFileManager.createFile(TABLES_TABLE);
        }

        if (!recordBasedFileManager.isExist(COLUMNS_TABLE)) {
            recordBasedFileManager.createFile(COLUMNS_TABLE);
        }

    }

    private RecordID insertCatalogTuple(String tableName, byte[] data) {

        /**
         *    FileHandle fileHandle;
         vector<Attribute> recordDescriptor;

         if (rbfm->openFile(tableName, fileHandle) == FAIL) {
         return FAIL;
         }

         prepareRecordDescriptor(tableName, recordDescriptor);

         if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
         return FAIL;
         }

         rbfm->closeFile(fileHandle);

         return SUCCESS;
         */

        return null;

    }


    private List<Attribute> getSystemTupleForTables() {
        List<Attribute> dataFiledList = new ArrayList<>();

        Attribute tableId = new Attribute("TABLE.ID", DataFiled.Types.INT, 4);
        Attribute tableName = new Attribute("TABLE.NAME", DataFiled.Types.STRING, 50);
        Attribute fileName = new Attribute("FILE.NAME", DataFiled.Types.STRING, 50);
        Attribute systemFlag = new Attribute("SYSTEM.FLAG", DataFiled.Types.BOOL, 1);

        dataFiledList.add(tableId);
        dataFiledList.add(tableName);
        dataFiledList.add(fileName);
        dataFiledList.add(systemFlag);
        return dataFiledList;
    }

    private List<Attribute> getSystemTupleForColumnTables() {
        List<Attribute> dataFiledList = new ArrayList<>();
        Attribute tableId = new Attribute("TABLE.ID", DataFiled.Types.INT, 4);
        Attribute tableName = new Attribute("COLUMN.NAME", DataFiled.Types.STRING, 50);
        Attribute columnType = new Attribute("COLUMN.TYPE", DataFiled.Types.STRING, 50);
        Attribute columnLength = new Attribute("COLUMN.LENGTH", DataFiled.Types.INT, 4);
        Attribute columnPosition = new Attribute("COLUMN.POSITION", DataFiled.Types.INT, 4);
        Attribute systemFlag = new Attribute("SYSTEM.FLAG", DataFiled.Types.BOOL, 1);

        dataFiledList.add(tableId);
        dataFiledList.add(tableName);
        dataFiledList.add(columnType);
        dataFiledList.add(columnLength);
        dataFiledList.add(columnPosition);
        dataFiledList.add(systemFlag);
        return dataFiledList;
    }


}
