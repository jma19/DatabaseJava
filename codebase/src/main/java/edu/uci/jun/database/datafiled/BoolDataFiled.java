package edu.uci.jun.database.datafiled;

import java.lang.Boolean;
import java.nio.ByteBuffer;

/**
 * Boolean data type which serializes to 1 byte.
 */
public class BoolDataFiled extends DataFiled {
    private boolean bool;

    /**
     * Construct an empty BoolDataFiled.
     */
    public BoolDataFiled() {
        this.bool = false;
    }

    /**
     * Construct a BoolDataFiled with value b.
     *
     * @param b the value of the BoolDataFiled
     */
    public BoolDataFiled(boolean b) {
        this.bool = b;
    }

    /**
     * Construct a BoolDataFiled from a byte buffer.
     *
     * @param buf the byte buffer source
     */
    public BoolDataFiled(byte[] buf) {
        if (buf.length != this.getSize()) {
            throw new DataFiledException("Wrong size buffer for boolean");
        }
        this.bool = (buf[0] != 0);
    }

    @Override
    public boolean getBool() {
        return this.bool;
    }

    @Override
    public void setBool(boolean b) {
        this.bool = b;
    }

    @Override
    public Types type() {
        return DataFiled.Types.BOOL;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (this == null)
            return false;
        if (this.getClass() != obj.getClass())
            return false;
        BoolDataFiled other = (BoolDataFiled) obj;
        return Boolean.compare(this.getBool(), other.getBool()) == 0;
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataFiledException("Invalid Comparison");
        }
        BoolDataFiled other = (BoolDataFiled) obj;
        return Boolean.compare(this.getBool(), other.getBool());
    }

    @Override
    public byte[] getBytes() {
        byte val = this.bool ? (byte) 1 : (byte) 0;
        return ByteBuffer.allocate(1).put(val).array();
    }

    @Override
    public int getSize() {
        return 1;
    }

    @Override
    public String toString() {
        if (this.bool) {
            return "true";
        } else {
            return "false";
        }
    }
}
