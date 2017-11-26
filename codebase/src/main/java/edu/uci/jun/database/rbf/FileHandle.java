package edu.uci.jun.database.rbf;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;

/**
 * Created by junm5 on 11/15/17.
 */
public class FileHandle implements Iterable<Page>, Closeable {

    private RandomAccessFile randomAccessFile;
    private String fileName;
    private boolean isOpen = false;

    public FileHandle(String fileName) {
        this.fileName = fileName;
    }

    public boolean open() {
        try {
            this.randomAccessFile = new RandomAccessFile(fileName, "rw");
            isOpen = true;
        } catch (IOException e) {
            throw new PageFileException("Could not open File: " + e.getMessage());
        }
        return true;
    }

    @Override
    public void close() {
        try {
            this.randomAccessFile.close();
            isOpen = false;
        } catch (IOException e) {
            throw new PageFileException("fail to close File: " + e.getMessage());
        }
    }

    public byte[] readPage(int pageNum) {
        if (!isOpen) {
            throw new PageFileException("file is not open !!!");
        }
        if (pageNum > getCurrentPageNum()) {
            throw new PageFileException(String.format("fail to read page due to outbound page number %d", pageNum));
        }
        Page page = new Page(randomAccessFile.getChannel(), pageNum, true);
        return page.readBytes();
    }

    public boolean writePage(int pageNum, byte[] data) {
        if (!isOpen) {
            throw new PageFileException("file is not open !!!");
        }
        Page page = new Page(randomAccessFile.getChannel(), pageNum, true);
        page.writeBytes(0, Page.pageSize, data);
        return true;
    }

    /**
     * append a page to the heap file
     *
     * @return
     */
    public boolean appendPage(byte[] data) {
        if (!isOpen) {
            throw new PageFileException("file is not open !!!");
        }
        int pageNum = getCurrentPageNum();
        return writePage(pageNum, data);
    }

    public int getCurrentPageNum() {
        if (!isOpen) {
            throw new PageFileException("file is not open !!!");
        }
        try {
            randomAccessFile.seek(randomAccessFile.length());
            long position = randomAccessFile.getFilePointer();
            return (int) Math.ceil(position * 1.0 / Page.pageSize);
        } catch (IOException e) {
            throw new PageFileException("fail to get current page number," + e);
        }
    }

    @Override
    public Iterator<Page> iterator() {
        return new PageIterator();
    }

    class PageIterator implements Iterator<Page> {
        private int currPageNum = 0;

        @Override
        public boolean hasNext() {
            int currentPageNum = getCurrentPageNum();
            return currPageNum < currentPageNum;
        }

        @Override
        public Page next() {
            return new Page(randomAccessFile.getChannel(), currPageNum, true);
        }
    }

    public FileChannel getFileChannel() {
        return randomAccessFile.getChannel();
    }

}

