package edu.uci.jun.database.rbf;

import java.util.Iterator;

/**
 * Created by junm5 on 11/15/17.
 */
public class PageIterator implements Iterator<Page> {
    private int currPageNum = 0;
    private FileHandle fileHandle;

    /**
     * @param fileHandle
     */
    public PageIterator(FileHandle fileHandle) {
        this.fileHandle = fileHandle;
    }

    @Override
    public boolean hasNext() {
        int currentPageNum = fileHandle.getCurrentPageNum();
        return currPageNum <= currentPageNum;
    }

    @Override
    public Page next() {
        return new Page(fileHandle.getRandomAccessFile().getChannel(), currPageNum, true);
    }
}
