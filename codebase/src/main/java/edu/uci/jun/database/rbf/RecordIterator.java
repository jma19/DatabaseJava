package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.table.Schema;

import java.util.Iterator;
import java.util.List;

/**
 * An implementation of Iterator that takes in a RecordID iterator provides iteration over Records
 */
public class RecordIterator implements Iterator<Record> {
    private static int currentPageNum = 0;
    private Iterator<RecordID> recordIDIter;
    private RecordBasedFileManager recordBasedFileManager;
    private List<DataFiled> fieldTypes;
    private Page currentPage;
    private int currentSlotNum;

    public RecordIterator(String tableName, List<DataFiled> fieldTypes, Iterator<RecordID> recIDIter) {
        this.recordBasedFileManager = new RecordBasedFileManager();
        this.recordIDIter = recIDIter;
        this.recordBasedFileManager.openFile(tableName + ".tab");
        this.fieldTypes = fieldTypes;
        this.currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, false);
        this.currentSlotNum = 0;
    }

    @Override
    public boolean hasNext() {
        return currentPageNum >= recordBasedFileManager.getFileHandle().getCurrentPageNum();
    }

    @Override
    public Record next() {
        short slotNum = currentPage.getSlotNum();
        Record record = null;
        while (currentSlotNum < slotNum) {
            RecordID recordID = new RecordID(currentPageNum, currentPageNum);
            try {
                record = recordBasedFileManager.getRecord(recordID, fieldTypes);
                currentSlotNum++;
                if (record != null) {
                    return record;
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
            if (currentPageNum == slotNum) {
                currentPageNum++;
                if (currentPageNum == recordBasedFileManager.getFileHandle().getCurrentPageNum()) {
                    return null;
                }
                currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, false);
                currentSlotNum = 0;
            }
        }
        return record;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

