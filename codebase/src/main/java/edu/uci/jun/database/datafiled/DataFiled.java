package edu.uci.jun.database.datafiled;

/**
 * Abstract DataFiled for all database primitives Currently supported: integers, booleans, floats,
 * fixed-length strings.
 * <p>
 * DataFields are also comparable allowing comparisons or sorting.
 * <p>
 * Provides default functionality for all DataFiled subclasses by assuming that the contained value is
 * not of the type specified.
 */
public abstract class DataFiled implements Comparable {

    /**
     * An enum with the current supported types.
     */
    public enum Types {
        BOOL, INT, FLOAT, STRING
    }

    public DataFiled() throws DataFiledException {
    }

    public DataFiled(boolean b) throws DataFiledException {
        throw new DataFiledException("not boolean type");
    }

    public DataFiled(int i) throws DataFiledException {
        throw new DataFiledException("not int type");
    }

    public DataFiled(float f) throws DataFiledException {
        throw new DataFiledException("not float type");
    }

    public DataFiled(String s, int len) throws DataFiledException {
        throw new DataFiledException("not String type");
    }

    public DataFiled(byte[] buf) throws DataFiledException {
        throw new DataFiledException("Not Implemented");
    }

    public boolean getBool() throws DataFiledException {
        throw new DataFiledException("not boolean type");
    }

    public int getInt() throws DataFiledException {
        throw new DataFiledException("not int type");
    }

    public float getFloat() throws DataFiledException {
        throw new DataFiledException("not float type");
    }

    public String getString() throws DataFiledException {
        throw new DataFiledException("not String type");
    }

    public void setBool(boolean b) throws DataFiledException {
        throw new DataFiledException("not boolean type");
    }

    public void setInt(int i) throws DataFiledException {
        throw new DataFiledException("not int type");
    }

    public void setFloat(float f) throws DataFiledException {
        throw new DataFiledException("not float type");
    }

    public void setString(String s, int len) throws DataFiledException {
        throw new DataFiledException("not string type");
    }

    /**
     * Returns the type of the DataFiled.
     *
     * @return the type from the Types enum
     * @throws DataFiledException
     */
    public Types type() throws DataFiledException {
        throw new DataFiledException("No type");
    }

    /**
     * Returns a byte array with the data contained by this DataFiled.
     *
     * @return a byte array
     * @throws DataFiledException
     */
    public byte[] getBytes() throws DataFiledException {
        throw new DataFiledException("Not Implemented");
    }

    /**
     * Returns the fixed size of this DataFiled.
     *
     * @return the size of the DataFiled
     * @throws DataFiledException
     */
    public int getSize() throws DataFiledException {
        throw new DataFiledException("Not Implemented");
    }

    public int compareTo(Object obj) throws DataFiledException {
        throw new DataFiledException("Not Implemented");
    }

    @Override
    public String toString() throws DataFiledException {
        throw new DataFiledException("Not Implemented");
    }
}
