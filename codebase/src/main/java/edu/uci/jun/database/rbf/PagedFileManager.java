package edu.uci.jun.database.rbf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * page file manager that is used to create/destroy heap file.
 * it also provides functions including read/write/append pages
 */
public class PagedFileManager {

    private static volatile PagedFileManager pageFileManager;

    private Map<String, FileHandle> map = new HashMap<>();

    /**
     * create page file manager instance
     *
     * @return PagedFileManager
     */
    public PagedFileManager getInstance() {
        if (pageFileManager == null) {
            synchronized (this) {
                if (pageFileManager == null) {
                    pageFileManager = new PagedFileManager();
                }
            }
        }
        return pageFileManager;
    }

    public boolean createFile(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            throw new PageFileException(String.format("File %s already exists", fileName));
        }
        try {
            return file.createNewFile();
        } catch (IOException e) {
            throw new PageFileException(String.format("fail to create file %s", fileName));
        }
    }

    public boolean destroyFile(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            throw new PageFileException(String.format("File %s doesn't exists", fileName));
        }
        return file.delete();
    }

    public boolean openFile(String fileName) {
        FileHandle fileHandle = new FileHandle(fileName);
        fileHandle.open();
        map.put(fileName, fileHandle);
        return true;
    }

    public boolean closeFile(String fileName) {
        FileHandle fileHandle = map.get(fileName);
        if (fileHandle == null) {
            return false;
        }
        fileHandle.close();
        return true;
    }

}

