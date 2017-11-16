package edu.uci.jun.database.datafiled;

import java.lang.String;
import java.nio.charset.Charset;

/**
 * Fixed-length String data type which serializes to UTF-8 bytes.
 */
public class StringDataField extends DataFiled {
  private String s;

  /**
   * Construct an empty StringDataField.
   */
  public StringDataField() {
    this.s = "";
  }

  /**
   * Construct a StringDataField with length len and value s.
   *
   * @param s the value of the StringDataField
   * @param len the length of the StringDataField
   */
  public StringDataField(String s, int len) {
		if (len < s.length()) {
    	this.s = s.substring(0, len);
    } else {
 			this.s = String.format("%-" + len + "s", s);
    }
  }

  /**
   * Construct a StringDataField from the bytes in buf.
   *
   * @param buf the byte buffer source
   */
  public StringDataField(byte[] buf) {
    this.s = new String(buf, Charset.forName("UTF-8"));
  }

  public StringDataField(int len) {
    this.s = "";

    for (int i = 0; i < len; i++) {
      this.s += " ";
    }
  }

  @Override
  public String getString() {
    return this.s;
  }

  @Override
  public void setString(String s, int len) {
		if (len < s.length()) {
    	this.s = s.substring(0, len);
    } else {
 			this.s = String.format("%-" + len + "s", s);
    }
  }

  @Override
  public Types type() {
    return DataFiled.Types.STRING;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (this == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    StringDataField other = (StringDataField) obj;
    return this.getString().equals(other.getString());
  }

  @Override
  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataFiledException("Invalid Comparsion");
    }
    StringDataField other = (StringDataField) obj;
    return this.getString().compareTo(other.getString());
  }

  @Override
  public byte[] getBytes() {
    return this.s.getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public int getSize() {
    return s.getBytes(Charset.forName("UTF-8")).length;
  }

  @Override
  public String toString() {
    return this.s;
  }
}
