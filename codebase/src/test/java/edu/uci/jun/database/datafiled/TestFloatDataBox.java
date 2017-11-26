package edu.uci.jun.database.datafiled;

import org.junit.Test;

import static org.junit.Assert.*;

/**
* @author  Sammy Sidhu
* @version 1.0
*/

public class TestFloatDataBox {
  @Test
  public void TestFloatDataBoxConstructor() {
    DataFiled first = new FloatDataField(9.9f);
    assertEquals(first.getFloat(), 9.9f, 1e-9f);

    DataFiled sec = new FloatDataField(-9.9f);
    assertEquals(sec.getFloat(), -9.9f, 1e-9f);
  }

  @Test
  public void TestFloatDataBoxSetters() {
    DataFiled first = new FloatDataField();
    first.setFloat(1.3f);
    assertEquals(first.getFloat(), 1.3f, 1e-9f);
    first.setFloat(-1.3f);
    assertEquals(first.getFloat(), -1.3f, 1e-9f);
  }

  @Test
  public void TestFloatDataBoxType() {
    DataFiled first = new FloatDataField();
    assertEquals(first.type(), DataFiled.Types.FLOAT);
  }

  @Test
  public void TestFloatDataBoxEquals() {
    DataFiled first = new FloatDataField(1.1f);
    DataFiled second = new FloatDataField(1.1f);
    assertEquals(first, second);
  }

  @Test
  public void TestFloatDataBoxCompare() {
    DataFiled first = new FloatDataField(1.1f);
    DataFiled second = new FloatDataField(1.2f);
    assertTrue(first.compareTo(second) == -1);
    first.setFloat(1.2f);
    assertTrue(first.compareTo(second) == 0);
    first.setFloat(1.3f);
    assertTrue(first.compareTo(second) == 1);
  }

  @Test
  public void TestFloatDataBoxSerialize() {
    DataFiled first = new FloatDataField(11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataFiled sec = new FloatDataField(b);
    assertEquals(first, sec);
  }

  @Test
  public void TestFloatDataBoxSerialize2() {
    DataFiled first = new FloatDataField(-11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataFiled sec = new FloatDataField(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataFiledException.class)
  public void TestFloatDataBoxString() {
    DataFiled first = new FloatDataField(1.1f);
    first.getString();
  }

  @Test(expected = DataFiledException.class)
  public void TestFloatDataBoxString2() {
    DataFiled first = new FloatDataField(1.1f);
    String s = "LOL";
    first.setString(s,s.length());
  }

  @Test(expected = DataFiledException.class)
  public void TestFloatDataBoxInt() {
    DataFiled first = new FloatDataField(1.1f);
    first.getInt();
  }

  @Test(expected = DataFiledException.class)
  public void TestFloatDataBoxInt2() {
    DataFiled first = new FloatDataField(1.1f);
    first.setInt(1);
  }
}
