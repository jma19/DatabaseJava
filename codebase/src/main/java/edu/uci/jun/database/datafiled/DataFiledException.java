package edu.uci.jun.database.datafiled;

/**
* Exception that is thrown for DataFiled errors such as type mismatches
*/
public class DataFiledException extends RuntimeException {

  public DataFiledException() {
    super();
  }

  public DataFiledException(String message) {
    super(message);
  }
}
