package edu.uci.jun.database.rbf;

/**
 * Exception thrown for errors while page file operation
 */
public class PageFileException extends RuntimeException {

    public PageFileException() {
    }

    public PageFileException(String message) {
        super(message);
    }
}
