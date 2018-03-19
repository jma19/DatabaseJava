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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static edu.uci.jun.database.datafiled.DataFiled.Types.*;

/**
 * 1. catalog stores table descriptions, and column descriptions
 * 2. for table descriptions, we have tableId, table name, file name, sys flag
 * 3. for column descriptions, we have
 * Created by junm5 on 11/22/17.
 */
public class RelationManager {
    private final static RelationManager instance = new RelationManager();

    public final static String CATALOG_PREFIX = "catalog";
    /**
     * The Constant LOGGER.
     */
    private static final Logger logger = LogManager.getLogger(RelationManager.class);


    private RelationManager() {
        init();
    }

    /***
     * when initiate RelationManager, check existence of the catalog dir
     */
    private void init() {
        File dir = new File(CATALOG_PREFIX);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    public static RelationManager getInstance() {
        return instance;
    }

    //file for storing table info
    // rmation
    public final static String TABLES_TABLE = CATALOG_PREFIX + "/tables.tab";
    //file for storing columns of a table
    public final static String COLUMNS_TABLE = CATALOG_PREFIX + "/columns.tab";
    //file for storing latest id
    public final static String TABLES_ID = CATALOG_PREFIX + "/tablesId.tab";

    private final static int EMPTY_TABLE_ID = -1;

    /***
     * create system catalog for table, and columns
     * @throws DatabaseException
     */
    public void createCatalog() throws DatabaseException {
        logger.debug("check catalog file....");
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();

        if (!recordBasedFileManager.isExist(TABLES_TABLE)) {
            logger.debug("table catalog doesn't exist, try to create");
            recordBasedFileManager.createFileIfNotExist(TABLES_TABLE);
            logger.debug("create table catalog successfully...");
        }

        if (!recordBasedFileManager.isExist(COLUMNS_TABLE)) {
            logger.debug("column catalog doesn't exist, try to create");
            recordBasedFileManager.createFileIfNotExist(COLUMNS_TABLE);
            logger.debug("create column catalog successfully...");
        }

        if (!recordBasedFileManager.isExist(TABLES_ID)) {
            logger.debug("id catalog doesn't exist, try to create");
            recordBasedFileManager.createFileIfNotExist(TABLES_ID);
            logger.debug("create id catalog successfully...");
        }

        try {
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
            List<List<DataFiled>> tuplesForColumnsTable = getTuplesForColumnsTable();
            try {
                for (List<DataFiled> dataFileds : tuplesForColumnsTable)
                    recordBasedFileManager.addRecord(dataFileds);
            } catch (DatabaseException e) {
                throw new DatabaseException("fail to create column table scheme");
            }
            recordBasedFileManager.closeFile(COLUMNS_TABLE);
            recordBasedFileManager.openFile(TABLES_ID);
            updateLatestTableId(0);
        } catch (Exception exp) {
            clearAllCatalogs();
        }
    }

    /**
     * delete all catalogs
     */
    public void clearAllCatalogs() {
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.destroyFile(TABLES_TABLE);
        recordBasedFileManager.destroyFile(COLUMNS_TABLE);
        recordBasedFileManager.destroyFile(TABLES_ID);
    }


    private int getLatestTableId() throws DatabaseException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(TABLES_ID))) {
            String data = bufferedReader.readLine();
            return data == null ? EMPTY_TABLE_ID : Integer.valueOf(data);
        } catch (IOException e) {
            throw new DatabaseException("fail to get latest table id", e);
        }
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
        checkNotNull(schema);
        checkNotNull(schema.getTableName(), "table name should not be null");
        checkNotNull(schema.getFieldTypes(), "filed type should not be null");
        checkNotNull(schema.getTableName(), "table name should not be null");
        RecordBasedFileManager recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.openFile(TABLES_TABLE);

        //Todo add check for table name

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
                    new StringDataField(fieldTypes.get(i).getType().getVal()),
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
        Schema systemTupleForColumnTables = getSystemSchemeForColumnsTable();
        RecordIterator columnIterator = recordBasedFileManager.scan(systemTupleForColumnTables, "TABLE_ID", QueryPlan.PredicateOperator.EQUALS, tableId, null);

        //then get table columns
        Map<Integer, Attribute> attriPosMap = new HashMap<>();

        while (columnIterator.hasNext()) {
            Record record = columnIterator.next();
            List<DataFiled> values = record.getValues();

            String columnName = ((StringDataField) values.get(1)).getString();
            String type = ((StringDataField) values.get(2)).getString();
            int length = ((IntDataField) values.get(3)).getInt();
            int position = ((IntDataField) values.get(4)).getInt();

            attriPosMap.put(position, new Attribute(columnName, DataFiled.Types.get(type), length));
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
            } else if (type.equals(INT)) {
                dataFileds.add(new IntDataField());
            } else if (type.equals(DataFiled.Types.STRING)) {
                dataFileds.add(new StringDataField(attribute.getLength()));
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
        List<DataFiled> values = Lists.newArrayList(new IntDataField(0), new StringDataField("System table"), new StringDataField("NULL"), new BoolDataFiled(true));
        return new Schema(TABLES_TABLE, fieldNames, values);
    }


    private List<List<DataFiled>> getTuplesForColumnsTable() {
        List<List<DataFiled>> cols = Lists.newArrayList();
        cols.add(Lists.newArrayList(new IntDataField(0), new StringDataField("table id"), new StringDataField(INT.getVal()),
                new IntDataField(50), new IntDataField(0), new BoolDataFiled(true)));

        cols.add(Lists.newArrayList(new IntDataField(0), new StringDataField("column name"), new StringDataField(STRING.getVal()),
                new IntDataField(50), new IntDataField(1), new BoolDataFiled(true)));

        cols.add(Lists.newArrayList(new IntDataField(0), new StringDataField("column type"), new StringDataField(STRING.getVal()),
                new IntDataField(50), new IntDataField(2), new BoolDataFiled(true)));

        cols.add(Lists.newArrayList(new IntDataField(0), new StringDataField("column length"), new StringDataField(INT.getVal()),
                new IntDataField(50), new IntDataField(3), new BoolDataFiled(true)));

        cols.add(Lists.newArrayList(new IntDataField(0), new StringDataField("system flag"), new StringDataField(BOOL.getVal()),
                new IntDataField(50), new IntDataField(4), new BoolDataFiled(true)));

        return cols;
    }

    private Schema getSystemSchemeForColumnsTable() {
        List<String> fieldNames = Lists.newArrayList("TABLE_ID", "COLUMN_NAME", "COLUMN_TYPE", "COLUMN_LENGTH", "COLUMN_POSITION", "SYSTEM_FLAG");
        List<DataFiled> values = Lists.newArrayList(new IntDataField(0), new StringDataField("system default column name"), new StringDataField(),
                new IntDataField(), new IntDataField(), new BoolDataFiled(true));
        return new Schema(COLUMNS_TABLE, fieldNames, values);
    }


}
