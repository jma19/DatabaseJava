package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * General-purpose wrapper for interacting with the memory-mapped bytes on a page.
 */
public class Page {
    public static final int pageSize = 4096;
    public static final int slotSize = 4;
    public static final int recordOffsetSize = 2;
    public static final int recordLengthSize = 2;
    public static final int slotNumSize = 2;
    public static final int freeOffsetSize = 2;

    private static final int slotNumOffset = 4092;
    private static final int freeSpaceOffset = 4094;

    private MappedByteBuffer pageData;
    private int pageNum;
    private boolean durable;

    /**
     * Create a new page using fc with at offset blockNum with virtual page number pageNum
     *
     * @param fc       the file channel for this Page
     * @param blockNum the block in the file for this page
     * @param pageNum  the virtual page number
     */
    public Page(FileChannel fc, int blockNum, int pageNum) {
        this(fc, blockNum, pageNum, true);
    }


    /**
     * Create a new page using fc with at offset blockNum with virtual page number pageNum
     *
     * @param fc      the file channel for this Page
     * @param pageNum the virtual page number
     */
    public Page(FileChannel fc, int pageNum, boolean durable) {
        this.pageNum = pageNum;
        this.durable = durable;
        PageAllocator.incrementCacheMisses();
        try {
            this.pageData = fc.map(FileChannel.MapMode.READ_WRITE, pageNum * Page.pageSize, Page.pageSize);
        } catch (IOException e) {
            throw new PageException("Can't map page: " + pageNum + " ; " + e.getMessage());
        }
    }


    public Page(FileChannel fc, int blockNum, int pageNum, boolean durable) {
        this.pageNum = pageNum;
        this.durable = durable;
        PageAllocator.incrementCacheMisses();
        try {
            this.pageData = fc.map(FileChannel.MapMode.READ_WRITE, blockNum * Page.pageSize, Page.pageSize);
        } catch (IOException e) {
            throw new PageException("Can't map page: " + pageNum + "at block: " + blockNum + " ; " + e.getMessage());
        }
    }

    /**
     * Reads num bytes from offset position into buf.
     *
     * @param position the offset in the page to read from
     * @param num      the number of bytes to read
     * @param buf      the buffer to put the bytes into
     */
    public void readBytes(int position, int num, byte[] buf) {
        if (Page.pageSize < position + num) {
            throw new PageException("readBytes is out of bounds");
        }
        if (buf.length < num) {
            throw new PageException("num bytes to read is longer than buffer");
        }
        pageData.position(position);
        pageData.get(buf, 0, num);
    }

    /**
     * Reads num bytes from offset position and returns them in a new byte array.
     *
     * @param position the offset in the page to read from
     * @param num      the number of bytes to read
     * @return a new byte array with the bytes read
     */
    public byte[] readBytes(int position, int num) {
        if (Page.pageSize < position + num) {
            throw new PageException("readBytes is out of bounds");
        }
        byte[] data = new byte[num];
        readBytes(position, num, data);
        return data;
    }

    /**
     * Read all the bytes.
     *
     * @return a new byte array with all the bytes in the file
     */
    public byte[] readBytes() {
        return readBytes(0, Page.pageSize);
    }

    /**
     * Read the single byte at offset position.
     *
     * @param position the offest in the page to read from
     * @return the byte at offset position
     */
    public byte readByte(int position) {
        if (position < 0 || position >= Page.pageSize) {
            throw new PageException("readByte is out of bounds of page");
        }
        return pageData.get(position);
    }

    /**
     * Write num bytes from buf at offset position.
     *
     * @param position the offest in the file to write to
     * @param num      the number of bytes to write
     * @param buf      the source for the write
     */
    public void writeBytes(int position, int num, byte[] buf) {
        if (buf.length < num) {
            throw new PageException("num bytes to write is longer than buffer");
        }

        if (position < 0 || num < 0) {
            throw new PageException("position or num can't be negative");
        }

        if (Page.pageSize < num + position) {
            throw new PageException("writeBytes would go out of bounds");
        }

        pageData.position(position);
        pageData.put(buf, 0, num);
    }

    /**
     * Write a single byte into the file at offset position.
     *
     * @param position the offset in the file to write to
     * @param b        the byte to write
     */
    public void writeByte(int position, byte b) {
        if (position < 0 || position >= Page.pageSize) {
            throw new PageException("readByte is out of bounds of page");
        }
        pageData.put(position, b);
    }

    /**
     * Write a 4-byte integer into the page at offset startPos.
     *
     * @param startPos the offset in the file to write to
     * @param value    the value to write
     */
    public void writeInt(int startPos, int value) {
        this.writeBytes(startPos, 4, ByteBuffer.allocate(4).putInt(value).array());
    }

    /**
     * Read a 4-byte integer from the page at offset startPos.
     *
     * @param startPos the offset in the file to read from
     * @return the 4-byte integer at startPos
     */
    public int readInt(int startPos) {
        return ByteBuffer.wrap(this.readBytes(startPos, 4)).getInt();
    }

    /**
     * Completely wipe (zero out) the page.
     */
    public void wipe() {
        byte[] zeros = new byte[Page.pageSize];
        this.writeBytes(0, Page.pageSize, zeros);
    }

    /**
     * Force the page to disk.
     */
    public void flush() {
        if (this.durable) {
            PageAllocator.incrementCacheMisses();
            this.pageData.force();
        }
    }

    /**
     * @return the virtual page number of this page
     */
    public int getPageNum() {
        return this.pageNum;
    }

    /**
     * @return the slot number in this page.
     */
    public short getSlotNum() {
        byte[] slotNum = this.readBytes(slotNumOffset, 2);
        return ByteBuffer.wrap(slotNum).asShortBuffer().get();
    }

    /**
     * @return the free space offset
     */
    public int getFreeSpaceOffset() {
        byte[] freeSpaceSpace = this.readBytes(freeSpaceOffset, 2);
        return ByteBuffer.wrap(freeSpaceSpace).getShort();
    }

    /**
     * @return the free space
     */
    public int getFreeSpace() {
        return pageSize - getFreeSpaceOffset() -
                slotSize * getSlotNum() - slotNumSize - freeOffsetSize;
    }

    /**
     * @param offset
     */
    public void updateFreeSpaceOffset(short offset) {
        this.writeBytes(freeSpaceOffset, freeOffsetSize, ByteBuffer.allocate(freeOffsetSize).putShort(offset).array());
    }

    /**
     * update the record offset for a given slot id
     *
     * @param slotId start from 0
     * @param val
     */
    public void updateRecordOffset(int slotId, short val) {
        int start = pageSize - freeOffsetSize - slotNumSize - (slotId + 1) * slotSize;
        this.writeBytes(start, recordOffsetSize, ByteBuffer.allocate(recordOffsetSize).putShort(val).array());
    }

    /**
     * @param slotId start from 0
     * @return record offset for a given slot id
     * @throws DatabaseException
     */
    public short getRecordOffset(int slotId) throws DatabaseException {
        short slotNum = getSlotNum();
        if (slotId >= slotNum || slotId < 0) {
            throw new DatabaseException("target slot number outbound");
        }
        int start = pageSize - freeOffsetSize - slotNumSize - (slotId + 1) * slotSize;
        return ByteBuffer.wrap(this.readBytes(start, 2)).getShort();
    }

    /**
     * @param slotId
     * @return record length for a given slot id
     */
    public short getRecordLength(int slotId) {
        int start = pageSize - freeOffsetSize - slotNumSize - (slotId + 1) * slotSize + recordOffsetSize;
        return ByteBuffer.wrap(this.readBytes(start, 2)).getShort();
    }

    /**
     * append a slot to slot dict*
     *
     * @param offsetOfRecord
     * @param lengthOfRecord
     */
    public void appendSlotToSlotSlotDict(short offsetOfRecord, short lengthOfRecord) {
        int start = pageSize - slotNumSize - freeOffsetSize - getSlotNum() * slotSize;
        this.writeBytes(start, recordOffsetSize, ByteBuffer.allocate(freeOffsetSize).putShort(offsetOfRecord).array());
        this.writeBytes(start + recordOffsetSize, recordLengthSize, ByteBuffer.allocate(recordLengthSize).putShort(lengthOfRecord).array());
    }

    /**
     * @param slotNumber
     */
    public void updateSlotNum(short slotNumber) {
        this.writeBytes(slotNumOffset, slotNumSize, ByteBuffer.allocate(slotNumSize).putShort(slotNumber).array());
    }
}
