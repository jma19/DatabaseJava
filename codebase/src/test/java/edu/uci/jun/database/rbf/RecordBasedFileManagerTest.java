package edu.uci.jun.database.rbf;

import edu.uci.jun.database.TableCreateUtils;
import edu.uci.jun.database.datafiled.DataFiled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by junm5 on 11/25/17.
 */
public class RecordBasedFileManagerTest {

    private RecordBasedFileManager recordBasedFileManager;

    @Before
    public void setUp() throws Exception {
        recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.createFileIfNotExist("student.tab");
        recordBasedFileManager.openFile("student.tab");
        recordBasedFileManager.addRecord(TableCreateUtils.generateRandomTuple());

    }

    @After
    public void tearDown() throws Exception {
        recordBasedFileManager.destroyFile("student.tab");
    }

    @Test
    public void testAddRecord() throws Exception {
        RecordID recordID = recordBasedFileManager.addRecord(TableCreateUtils.generateRandomTuple());
        assertTrue(recordID != null);
        System.out.println(recordID);
    }


    @Test
    public void testGetRecord() throws Exception {
        List<DataFiled> values = TableCreateUtils.generateRandomTuple();
        RecordID recordID = recordBasedFileManager.addRecord(values);
        Record record = recordBasedFileManager.getRecord(recordID, TableCreateUtils.getSchema().getFieldTypes());
        List<DataFiled> result = record.getValues();
        assertTrue(values.equals(result));
    }

    @Test
    public void testDeleteRecord() throws Exception {
        List<DataFiled> values = TableCreateUtils.generateRandomTuple();
        RecordID recordID = recordBasedFileManager.addRecord(values);
        Record record = recordBasedFileManager.deleteRecord(recordID, TableCreateUtils.getSchema().getFieldTypes());
        assertTrue(values.equals(record.getValues()));

        Record record1 = recordBasedFileManager.getRecord(recordID, TableCreateUtils.getSchema().getFieldTypes());
        assertTrue(record1 == null);
    }
}