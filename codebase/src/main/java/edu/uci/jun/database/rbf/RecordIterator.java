package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.DataFiled;

import java.util.Iterator;
import java.util.List;

/**
 * An implementation of Iterator that takes in a RecordID iterator provides iteration over Records
 */
public class RecordIterator implements Iterator<Record> {
    private int currentPageNum = 0;
    private RecordBasedFileManager recordBasedFileManager;
    private List<DataFiled> fieldTypes;
    private Page currentPage;
    private int currentSlotNum;

    public RecordIterator(RecordBasedFileManager recordBasedFileManager, List<DataFiled> fieldTypes) {
        this.recordBasedFileManager = recordBasedFileManager;
        this.fieldTypes = fieldTypes;
        this.currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, true);
        this.currentSlotNum = 0;
    }

    /**
     * @return
     */
    @Override
    public boolean hasNext() {
        return currentPageNum < recordBasedFileManager.getFileHandle().getCurrentPageNum()
                && currentSlotNum < currentPage.getSlotNum();
    }


    /**
     * @return next record which is not null
     */
    @Override
    public Record next() {
        short slotNum = currentPage.getSlotNum();
        Record record = null;
        while (currentSlotNum < slotNum) {
            RecordID recordID = new RecordID(currentPageNum, currentSlotNum);
            try {
                record = recordBasedFileManager.getRecord(recordID, fieldTypes);
                currentSlotNum++;
                if (currentSlotNum == slotNum) {
                    currentPageNum++;
                    currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, false);
                    currentSlotNum = 0;
                }
                if (record != null) {
                    return record;
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            }

        }
        return record;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

