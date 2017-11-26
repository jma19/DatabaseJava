package edu.uci.jun.database.rm;

import com.google.common.collect.Lists;
import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.BoolDataFiled;
import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.datafiled.IntDataField;
import edu.uci.jun.database.datafiled.StringDataField;
import edu.uci.jun.database.rbf.RecordBasedFileManager;
import edu.uci.jun.database.rbf.RecordID;
import edu.uci.jun.database.table.Schema;

import java.io.*;
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
    //file for storing latest id
    private final static String TABLES_ID = "tablesId.tab";

    private final static int EMPTY_TABLE_ID = -1;

    /**
     * create system catalog includes: tables, columns
     */
    public void createCatalog() throws DatabaseException {
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();

        if (!recordBasedFileManager.isExist(TABLES_TABLE)) {
            recordBasedFileManager.createFileIfNotExist(TABLES_TABLE);
        }

        if (!recordBasedFileManager.isExist(COLUMNS_TABLE)) {
            recordBasedFileManager.createFileIfNotExist(COLUMNS_TABLE);
        }

        if (!recordBasedFileManager.isExist(TABLES_ID)) {
            recordBasedFileManager.createFileIfNotExist(TABLES_ID);
        }
        int latestTableId = getLatestTableId();

        if (latestTableId != EMPTY_TABLE_ID) {
            return;
        }

        recordBasedFileManager.openFile(TABLES_TABLE);
        Schema systemSchemeForTables = getSystemSchemeForTables();
        try {
            recordBasedFileManager.addRecord(systemSchemeForTables.getFieldTypes());
        } catch (DatabaseException e) {
            throw new DatabaseException("fail to create tables scheme");
        }
        recordBasedFileManager.closeFile(TABLES_TABLE);

        recordBasedFileManager.openFile(COLUMNS_TABLE);
        Schema systemTupleForColumnTables = getSystemTupleForColumnTables();
        try {
            recordBasedFileManager.addRecord(systemTupleForColumnTables.getFieldTypes());
        } catch (DatabaseException e) {
            throw new DatabaseException("fail to create column table scheme");
        }
        recordBasedFileManager.closeFile(COLUMNS_TABLE);
    }

    private int getLatestTableId() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(TABLES_ID))) {
            String data = bufferedReader.readLine();
            return data == null ? EMPTY_TABLE_ID : Integer.valueOf(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return EMPTY_TABLE_ID;
    }

    private void updateLatestTableId(int tableId) throws DatabaseException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(TABLES_ID))) {
            bufferedWriter.write(String.valueOf(tableId));
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new DatabaseException("fail to update latest table id");
        }
    }

    /**
     * write two system catalog file
     * update latest tableId
     *
     * @param schema
     */
    public void createTable(Schema schema) throws DatabaseException {
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.openFile(TABLES_TABLE);
        int tableId = getLatestTableId() + 1;
        updateLatestTableId(tableId);

        //update the table scheme
        List<DataFiled> tableScheme = Lists.newArrayList(
                new IntDataField(tableId),
                new StringDataField(schema.getTableName(), 50),
                new StringDataField(schema.getTableName() + ".tab", 50),
                new BoolDataFiled(false)
        );

        recordBasedFileManager.addRecord(tableScheme);
        recordBasedFileManager.closeFile(TABLES_TABLE);

        //update the column scheme
        recordBasedFileManager.openFile(COLUMNS_TABLE);
        List<String> fieldNames = schema.getFieldNames();
        List<DataFiled> fieldTypes = schema.getFieldTypes();
        for (int i = 0; i < fieldNames.size(); i++) {
            List<DataFiled> columnScheme = Lists.newArrayList(
                    new IntDataField(tableId),
                    new StringDataField(fieldNames.get(i)),
                    new IntDataField(fieldTypes.get(i).getType().getVal()),
                    new IntDataField(fieldTypes.get(i).getSize()),
                    new IntDataField(i),
                    new BoolDataFiled(false)
            );
            recordBasedFileManager.addRecord(columnScheme);
        }
        recordBasedFileManager.closeFile(COLUMNS_TABLE);
    }


    public Schema getTable(String tableName) {
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.openFile(TABLES_TABLE);
        return null;
    }

    public void deleteTable(String tableName) {

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
        //

        return null;

    }

    public void scan(String tableName, String conditionAttribute) {

    }


    private Schema getSystemSchemeForTables() {

        List<String> fieldNames = Lists.newArrayList("TABLE_ID", "TABLE_NAME", "FILE_NAME", "SYSTEM_FLAG");
        List<DataFiled> values = Lists.newArrayList(new IntDataField(), new StringDataField(50), new StringDataField(50), new BoolDataFiled(true));
        return new Schema(TABLES_TABLE, fieldNames, values);
    }

    private Schema getSystemTupleForColumnTables() {
        List<String> fieldNames = Lists.newArrayList("TABLE_ID", "COLUMN_NAME", "COLUMN_TYPE", "COLUMN_TYPE", "COLUMN_LENGTH", "COLUMN_POSITION", "SYSTEM_FLAG");
        List<DataFiled> values = Lists.newArrayList(new IntDataField(), new StringDataField(50), new StringDataField(50),
                new IntDataField(), new IntDataField(), new BoolDataFiled(true));
        return new Schema(COLUMNS_TABLE, fieldNames, values);
    }


}
