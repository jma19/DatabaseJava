package edu.uci.jun.database.rbf;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

/**
 * Created by junm5 on 11/15/17.
 */
public class FileHandle implements Iterable<Page>, Closeable {

    private RandomAccessFile randomAccessFile;

    private String fileName;

    public FileHandle(String fileName) {
        this.fileName = fileName;
    }

    public boolean open() {
        try {
            this.randomAccessFile = new RandomAccessFile(fileName, "rw");
        } catch (IOException e) {
            throw new PageFileException("Could not open File: " + e.getMessage());
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        this.randomAccessFile.close();
    }

    public boolean writePage(int pageNum, byte[] data) {
        Page page = new Page(randomAccessFile.getChannel(), pageNum, true);
        page.writeBytes(pageNum * Page.pageSize, Page.pageSize, data);
        return true;
    }

    /**
     * append a page to the heap file
     *
     * @return
     */
    public boolean appendPage(byte[] data) {
        int pageNum = getPageNum();
        return writePage(pageNum + 1, data);
    }

    public int getPageNum() {
        try {
            randomAccessFile.seek(randomAccessFile.length());
            long position = randomAccessFile.getFilePointer();
            return (int) Math.ceil(position / Page.pageSize);
        } catch (IOException e) {
            throw new PageFileException("fail to get current page number," + e);
        }
    }

    @Override
    public Iterator<Page> iterator() {
        return null;
    }
}
