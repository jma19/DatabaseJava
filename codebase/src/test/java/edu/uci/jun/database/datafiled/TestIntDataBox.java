package edu.uci.jun.database.datafiled;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestIntDataBox {
  @Test
  public void TestIntDataBoxConstructor() {
    DataFiled first = new IntDataField(99);
    assertEquals(first.getInt(), 99);

    DataFiled sec = new IntDataField(-99);
    assertEquals(sec.getInt(), -99);
  }

  @Test
  public void TestIntDataBoxSetters() {
    DataFiled first = new IntDataField();
    first.setInt(13);
    assertEquals(first.getInt(), 13);
    first.setInt(-13);
    assertEquals(first.getInt(), -13);
  }

  @Test
  public void TestIntDataBoxType() {
    DataFiled first = new IntDataField();
    assertEquals(first.type(), DataFiled.Types.INT);
  }

  @Test
  public void TestIntDataBoxEquals() {
    DataFiled first = new IntDataField(11);
    DataFiled second = new IntDataField(11);
    assertEquals(first, second);
  }

  @Test
  public void TestIntDataBoxCompare() {
    DataFiled first = new IntDataField(11);
    DataFiled second = new IntDataField(12);
    assertTrue(first.compareTo(second) == -1);
    first.setInt(12);
    assertTrue(first.compareTo(second) == 0);
    first.setInt(13);
    assertTrue(first.compareTo(second) == 1);
  }

  @Test
  public void TestIntDataBoxSerialize() {
    DataFiled first = new IntDataField(11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataFiled sec = new IntDataField(b);
    assertEquals(first, sec);
  }

  @Test
  public void TestIntDataBoxSerialize2() {
    DataFiled first = new IntDataField(-11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataFiled sec = new IntDataField(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataFiledException.class)
  public void TestIntDataBoxString() {
    DataFiled first = new IntDataField(11);
    first.getString();
  }

  @Test(expected = DataFiledException.class)
  public void TestIntDataBoxString2() {
    DataFiled first = new IntDataField(11);
    String s = "LOL";
    first.setString(s,s.length());
  }

  @Test(expected = DataFiledException.class)
  public void TestIntDataBoxFloat() {
    DataFiled first = new IntDataField(11);
    first.getFloat();
  }

  @Test(expected = DataFiledException.class)
  public void TestIntDataBoxFloat2() {
    DataFiled first = new IntDataField(11);
    first.setFloat(1.1f);
  }
}
