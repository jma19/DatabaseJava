package edu.uci.jun.database.rbf;

/**
 * Exception thrown for errors while paging.
 *
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS PACKAGE.
 */
public class PageException extends RuntimeException {

  public PageException() {
    super();
  }

  public PageException(String message) {
    super(message);
  }
}

