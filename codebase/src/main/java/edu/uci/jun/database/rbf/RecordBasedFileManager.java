package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.table.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by junm5 on 11/15/17.
 */
public class RecordBasedFileManager {
    private RecordIterator recordIterator;
    private FileHandle fileHandle;

    public RecordBasedFileManager() {
    }

    /**
     * create a new file
     *
     * @param fileName
     * @return
     */
    public boolean createFileIfNotExist(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            return true;
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new PageFileException("fail to create file");
        }
        return true;
    }

    public boolean isExist(String fileName) {
        File file = new File(fileName);
        return file.exists();
    }

    /**
     * @param fileName
     * @return
     */
    public boolean openFile(String fileName) {
        fileHandle = new FileHandle(fileName);
        return fileHandle.open();
    }

    public void closeFile(String fileName) {
        fileHandle.close();
    }

    /**
     * @param fileName
     * @return
     */
    public boolean destroyFile(String fileName) {
        File file = new File(fileName);
        return file.delete();
    }


    /**
     * Adds a new record to this table. The record should be added to the first
     * free slot of the first free page if one exists, otherwise a new page should
     * be allocated and the record should be placed in the first slot of that
     * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
     * Make sure to update this.stats, this.freePages, and this.numRecords as
     * necessary.
     *
     * @param values the values of the record being added
     * @return the RecordID of the added record
     * @throws DatabaseException if the values passed in to this method do not
     *                           correspond to the schema of this table
     */
    public RecordID addRecord(List<DataFiled> values) throws DatabaseException {
        short sizeOfNullIndicator = getNullIndicatorSize(values);
        byte[] nullIndicator = new byte[sizeOfNullIndicator];

        short[] filedOffsets = new short[values.size() + 1];
        short offset = (short) (sizeOfNullIndicator + 2 * filedOffsets.length);
        filedOffsets[0] = offset;
        byte flag = 0x10;
        int nullIndex = 0;

        for (int i = 1; i <= values.size(); i++) {
            DataFiled item = values.get(i - 1);
            if (item == null) {
                nullIndicator[nullIndex] |= flag;
            } else {
                offset += item.getSize();
            }
            filedOffsets[i] = offset;
            if (i % 8 == 0) {
                flag = 0x10;
                nullIndex++;
            } else {
                flag = (byte) (flag >>> 1);
            }
        }


        int currentPageNum = fileHandle.getCurrentPageNum();
        Page page = new Page(fileHandle.getFileChannel(), currentPageNum == 0 ? 0 : currentPageNum - 1, true);
        int costOfRecord = offset + Page.slotSize;

        if (costOfRecord > Page.pageSize) {
            throw new DatabaseException("Record is too large to fit");
        }
        //allocate a new page to write
        if (costOfRecord > page.getFreeSpace()) {
            page = new Page(fileHandle.getFileChannel(), currentPageNum, true);
        }
        byte[] bytesOfRecord = getBytesOfRecord(values, offset, nullIndicator, filedOffsets);

        int freeSpaceOffset = page.getFreeSpaceOffset();
        page.writeBytes(freeSpaceOffset, bytesOfRecord.length, bytesOfRecord);

        page.updateSlotNum((short) (page.getSlotNum() + 1));
        page.updateFreeSpaceOffset((short) (freeSpaceOffset + bytesOfRecord.length));
        page.appendSlotToSlotSlotDict((short) freeSpaceOffset, (short) bytesOfRecord.length);
        RecordID recordID = new RecordID(page.getPageNum(), page.getSlotNum() - 1);
        return recordID;
    }

    /**
     * @param values
     * @param totalSize
     * @param nullIndicator
     * @param fieldOffSet
     * @return the bytes for an record, it consist of three parts, null indicator, field of offset, and real data value
     */
    private byte[] getBytesOfRecord(List<DataFiled> values, int totalSize, byte[] nullIndicator, short[] fieldOffSet) {
        byte[] data = new byte[totalSize];
        int index = 0;
        for (byte item : nullIndicator) {
            data[index++] = item;
        }
        for (short value : fieldOffSet) {
            byte[] bytes = getBytes(value);
            for (byte item : bytes) {
                data[index++] = item;
            }
        }
        for (DataFiled dataFiled : values) {
            if (dataFiled != null) {
                byte[] bytes = dataFiled.getBytes();
                for (byte item : bytes) {
                    data[index++] = item;
                }
            }
        }
        return data;
    }


    private byte[] getBytes(short value) {
        return ByteBuffer.allocate(2).putShort(value).array();
    }

    public short getNullIndicatorSize(List<DataFiled> values) {
        return (short) Math.ceil(values.size() / 8.0);
    }

    /**
     * Deletes the record specified by rid from the table. Make sure to update
     * this.stats, this.freePages, and this.numRecords as necessary.
     *
     * @param rid the RecordID of the record to delete
     * @return the Record referenced by rid that was removed
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record deleteRecord(RecordID rid, List<DataFiled> fieldTypes) throws DatabaseException {
        checkPageNum(rid);
        Page page = new Page(fileHandle.getFileChannel(), rid.getPageNum(), true);
        short recordOffset = page.getRecordOffset(rid.getEntryNumber());
        if (recordOffset == -1) {
            throw new DatabaseException("record does not exist");
        }

        Record record = getRecord(rid, fieldTypes);
        page.updateRecordOffset(rid.getEntryNumber(), (short) (-1));
        return record;
    }

    private void checkPageNum(RecordID rid) throws DatabaseException {
        int currentPageNum = fileHandle.getCurrentPageNum();
        if (rid.getPageNum() >= currentPageNum) {
            throw new DatabaseException("page number out of bound");
        }
    }


    /**
     * Retrieves a record from the table.
     *
     * @param rid the RecordID of the record to retrieve
     * @return the Record referenced by rid
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record getRecord(RecordID rid, List<DataFiled> fieldTypes) throws DatabaseException {
        checkPageNum(rid);
        int pageNum = rid.getPageNum();
        Page page = new Page(fileHandle.getFileChannel(), pageNum, true);
        short recordOffset = page.getRecordOffset(rid.getEntryNumber());

        if (recordOffset == -1) {
            return null;
        }

        short recordLength = page.getRecordLength(rid.getEntryNumber());
        byte[] records = page.readBytes(recordOffset, recordLength);

        short[] fieldOffset = getFieldOffset(fieldTypes, records);

        short start = fieldOffset[0];
        List<DataFiled> result = new ArrayList<>();

        for (int i = 0; i < fieldTypes.size(); i++) {
            short end = fieldOffset[i + 1];
            if (start == end) {
                result.add(null);
            } else {
                DataFiled dataFiled = fieldTypes.get(i);
                byte[] bytes = Arrays.copyOfRange(records, start, end);
                if (dataFiled instanceof StringDataField) {
                    result.add(new StringDataField(bytes));
                } else if (dataFiled instanceof FloatDataField) {
                    result.add(new FloatDataField(bytes));
                } else if (dataFiled instanceof IntDataField) {
                    result.add(new IntDataField(bytes));
                } else if (dataFiled instanceof BoolDataFiled) {
                    result.add(new BoolDataFiled(bytes));
                }
            }
            start = end;
        }
        return new Record(result);
    }

    /**
     * @param dataFields
     * @param record
     * @return indicator of a byte
     */
    private byte[] getIndicator(List<DataFiled> dataFields, byte[] record) {
        short nullIndicatorSize = getNullIndicatorSize(dataFields);
        return Arrays.copyOf(record, nullIndicatorSize);
    }

    /**
     * @param dataFields
     * @param record
     * @return filed offset for a given record
     */
    private short[] getFieldOffset(List<DataFiled> dataFields, byte[] record) {
        short[] fieldOffSet = new short[dataFields.size() + 1];
        short nullIndicatorSize = getNullIndicatorSize(dataFields);
        int offset = nullIndicatorSize;

        for (int i = 0; i < fieldOffSet.length; i++) {
            short temp = ByteBuffer.wrap(Arrays.copyOfRange(record, offset, offset + 2)).getShort();
            fieldOffSet[i] = temp;
            offset += 2;
        }
        return fieldOffSet;
    }


    /**
     * Updates an existing record with new values and returns the old version of the record.
     * Make sure to update this.stats as necessary.
     *
     * @param values the new values of the record
     * @param rid    the RecordID of the record to update
     * @return the old version of the record
     * @throws DatabaseException if rid does not correspond to a valid record or
     *                           if the values do not correspond to the schema of this table
     */
    public Record updateRecord(List<DataFiled> values, RecordID rid) throws DatabaseException {
        // TODO: implement me!
        return null;
    }


    public RecordIterator scan(List<DataFiled> recordDesciptor) {
        return null;
    }

    /**
     * @return FileHandle
     */
    public FileHandle getFileHandle() {
        return this.fileHandle;
    }

}
