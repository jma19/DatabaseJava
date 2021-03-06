package edu.uci.jun.database;

public class DatabaseException extends Exception {
    private String message;

    public DatabaseException(String message) {
        this.message = message;
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
