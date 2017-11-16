package edu.uci.jun.database.datafiled;
import java.lang.Float;
import java.nio.ByteBuffer;

/**
 * Float data type which serializes to 14 bytes.
 */
public class FloatDataField extends DataFiled {
  private float f;

  /**
   * Construct an empty FloatDataField with value 0.
   */
  public FloatDataField() {
    this.f = 0.0f;
  }

  /**
   * Construct an empty FloatDataField with value f.
   *
   * @param f the value of the FloatDataField
   */
  public FloatDataField(float f) {
    this.f = f;
  }

  /**
   * Construct a FloatDataField from the bytes in buf
   *
   * @param buf the bytes to construct the FloatDataField from
   */
  public FloatDataField(byte[] buf) {
    if (buf.length != this.getSize()) {
      throw new DataFiledException("Wrong size buffer for float");
    }
    this.f = ByteBuffer.wrap(buf).getFloat();
  }

  @Override
  public float getFloat() {
    return this.f;
  }

  @Override
  public void setFloat(float f) {
    this.f = f;
  }

  @Override
  public Types type() {
    return DataFiled.Types.FLOAT;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (this == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    FloatDataField other = (FloatDataField) obj;
    return this.getFloat() == other.getFloat();
  }

  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataFiledException("Invalid Comparsion");
    }
    FloatDataField other = (FloatDataField) obj;
    return Float.compare(this.getFloat(), other.getFloat());
  }

  @Override
  public byte[] getBytes() {
    return ByteBuffer.allocate(4).putFloat(this.f).array();
  }

  @Override
  public int getSize() {
    return 4;
  }

  @Override
  public String toString() {
    return "" + this.f;
  }
}

