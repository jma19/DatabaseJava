package edu.uci.jun.database.rm;

import com.google.common.collect.Lists;
import com.oracle.tools.packager.Log;
import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.query.QueryPlan;
import edu.uci.jun.database.rbf.Record;
import edu.uci.jun.database.rbf.RecordBasedFileManager;
import edu.uci.jun.database.rbf.RecordIterator;
import edu.uci.jun.database.table.Schema;

import java.io.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

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
        //first get table_id
        recordBasedFileManager.openFile(TABLES_TABLE);
        Schema systemSchemeForTables = getSystemSchemeForTables();

        RecordIterator scan = recordBasedFileManager.scan(systemSchemeForTables, "TABLE_NAME", QueryPlan.PredicateOperator.EQUALS, new StringDataField(tableName), null);

        if (!scan.hasNext()) {
            Log.debug(String.format("the table %s doesn't exists..", tableName));
            return null;
        }

        IntDataField tableId = null;
        while (scan.hasNext()) {
            Record next = scan.next();
            tableId = (IntDataField) next.getValues().get(0);
            break;
        }
        checkNotNull(tableId, "table ID should not be null!!");
        Schema systemTupleForColumnTables = getSystemTupleForColumnTables();
        RecordIterator columnIterator = recordBasedFileManager.scan(systemTupleForColumnTables, "TABLE_ID", QueryPlan.PredicateOperator.EQUALS, tableId, null);

        //then get table columns
        Map<Integer, Attribute> attriPosMap = new HashMap<>();

        while (columnIterator.hasNext()) {
            Record record = columnIterator.next();
            List<DataFiled> values = record.getValues();
            String columnName = ((StringDataField) values.get(1)).getString();
            int type = ((IntDataField) values.get(2)).getInt();
            int length = ((IntDataField) values.get(3)).getInt();
            int position = ((IntDataField) values.get(4)).getInt();
            attriPosMap.put(position, new Attribute(columnName, DataFiled.Types.valueOf(type), length));
        }

        List<String> filedNames = new ArrayList<>();
        List<DataFiled> dataFileds = new ArrayList<>();

        for (int i = 0; i < attriPosMap.size(); i++) {
            Attribute attribute = attriPosMap.get(i);
            filedNames.add(attribute.getName());
            DataFiled.Types type = attribute.getType();
            if (type.equals(DataFiled.Types.BOOL)) {
                dataFileds.add(new BoolDataFiled());
            } else if (type.equals(DataFiled.Types.FLOAT)) {
                dataFileds.add(new FloatDataField());
            } else if (type.equals(DataFiled.Types.INT)) {
                dataFileds.add(new IntDataField());
            } else if (type.equals(DataFiled.Types.STRING)) {
                dataFileds.add(new StringDataField());
            }
        }
        return new Schema(tableName, filedNames, dataFileds);
    }

    public void deleteTable(String tableName) {

    }

    /**
     * RC RecordBasedFileManager::scan(FileHandle &fileHandle,
     * const vector<Attribute> &recordDescriptor,
     * const string &conditionAttribute,
     * const CompOp compOp,
     * const void *value,
     * const vector<string> &attributeNames,
     * RBFM_ScanIterator &rbfm_ScanIterator)
     *
     * @param tableName
     * @param predicateOperator
     * @param conditionAttribute
     */
    public void scan(String tableName, QueryPlan.PredicateOperator predicateOperator, String conditionAttribute) {

    }


    private Schema getSystemSchemeForTables() {
        List<String> fieldNames = Lists.newArrayList("TABLE_ID", "TABLE_NAME", "FILE_NAME", "SYSTEM_FLAG");
        List<DataFiled> values = Lists.newArrayList(new IntDataField(), new StringDataField(50), new StringDataField(50), new BoolDataFiled(true));
        return new Schema(TABLES_TABLE, fieldNames, values);
    }

    private Schema getSystemTupleForColumnTables() {
        List<String> fieldNames = Lists.newArrayList("TABLE_ID", "COLUMN_NAME", "COLUMN_TYPE", "COLUMN_LENGTH", "COLUMN_POSITION", "SYSTEM_FLAG");
        List<DataFiled> values = Lists.newArrayList(new IntDataField(), new StringDataField(50),
                new IntDataField(), new IntDataField(), new BoolDataFiled(true));
        return new Schema(COLUMNS_TABLE, fieldNames, values);
    }


}
