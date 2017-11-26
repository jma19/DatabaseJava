package edu.uci.jun.database.rbf;

import edu.uci.jun.database.TableCreateUtils;
import edu.uci.jun.database.datafiled.DataFiled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by junm5 on 11/25/17.
 */
public class RecordIteratorTest {

    private RecordIterator recordIterator;
    private RecordBasedFileManager recordBasedFileManager;
    private static final int recordNum = 1000;

    @Before
    public void setUp() throws Exception {
        recordBasedFileManager = new RecordBasedFileManager();
        recordBasedFileManager.createFileIfNotExist(TableCreateUtils.tableName);
        recordBasedFileManager.openFile(TableCreateUtils.tableName);

        for (int i = 0; i < recordNum; i++) {
            List<DataFiled> values = TableCreateUtils.generateRandomTuple();
            RecordID recordID = recordBasedFileManager.addRecord(values);
            assertTrue(recordID != null);
        }
        recordIterator = new RecordIterator(recordBasedFileManager, TableCreateUtils.getSchema().getFieldTypes());
        System.out.println(String.format("page number ---> %s", recordBasedFileManager.getFileHandle().getCurrentPageNum()));
    }

    @After
    public void tearDown() throws Exception {
        recordBasedFileManager.destroyFile(TableCreateUtils.tableName);
    }

    @Test
    public void testRecordIterator() throws Exception {
        int count = 0;
        while (recordIterator.hasNext()) {
            recordIterator.next();
            count++;
        }
        assertTrue(count == recordNum);
    }

}