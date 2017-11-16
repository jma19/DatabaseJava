package edu.uci.jun.database.datafiled;
import java.lang.Integer;
import java.nio.ByteBuffer;

/**
 * Integer data type which serializes to 4 bytes
 */
public class IntDataField extends DataFiled {
  private int i;

  /**
   * Construct an empty IntDataField with value 0.
   */
  public IntDataField() {
    this.i = 0;
  }

  /**
   * Constructs an IntDataField with value i.
   *
   * @param i the value of the IntDataField
   */
  public IntDataField(int i) {
    this.i = i;
  }

  /**
   * Construct an IntDataField from the bytes in buf.
   *
   * @param buf the byte buffer source
   */
  public IntDataField(byte[] buf) {
    if (buf.length != this.getSize()) {
      throw new DataFiledException("Wrong size buffer for int");
    }
    this.i = ByteBuffer.wrap(buf).getInt();
  }

  @Override
  public int getInt() {
    return this.i;
  }

  @Override
  public void setInt(int i) {
    this.i = i;
  }

  @Override
  public Types type() {
    return DataFiled.Types.INT;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (this == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    IntDataField other = (IntDataField) obj;
    return this.getInt() == other.getInt();
  }

  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataFiledException("Invalid Comparision");
    }
    IntDataField other = (IntDataField) obj;
    return Integer.compare(this.getInt(), other.getInt());
  }

  @Override
  public byte[] getBytes() {
    return ByteBuffer.allocate(4).putInt(this.i).array();
  }

  @Override
  public int getSize() {
    return 4;
  }

  @Override
  public String toString() {
    return "" + this.i;
  }
}
