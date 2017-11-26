package edu.uci.jun.database.rbf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * Created by junm5 on 11/22/17.
 */
public class FileHandleTest {

    private final String testFileName = "test.tab";

    @Before
    public void setUp() throws Exception {
        File file = new File(testFileName);
        file.createNewFile();
    }

    @After
    public void tearDown() throws Exception {
        File file = new File(testFileName);
        file.delete();
    }

    //if not open file, and then operate file.
    @Test(expected = Exception.class)
    public void testAppendPageThrowError() throws Exception {
        FileHandle fileHandle = new FileHandle(testFileName);
        fileHandle.getCurrentPageNum();

    }

    @Test
    public void testAppendPage() throws Exception {
        FileHandle fileHandle = new FileHandle(testFileName);
        fileHandle.open();
        int preCurrentNumber = fileHandle.getCurrentPageNum();
        byte[] data = new byte[Page.pageSize];
        assertTrue(fileHandle.appendPage(data));
        int currentPageNum = fileHandle.getCurrentPageNum();
        assertTrue(currentPageNum == preCurrentNumber + 1);
    }
}