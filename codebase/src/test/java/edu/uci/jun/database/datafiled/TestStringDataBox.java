package edu.uci.jun.database.datafiled;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestStringDataBox {

 	@Test
  public void TestStringDataBoxConstructor() {
    DataFiled first = new StringDataField();
    assertEquals(first.getString(), "");

    DataFiled sec = new StringDataField("hello", 5);
    assertEquals(sec.getString(), "hello");

    DataFiled third = new StringDataField("hello", 3);
    assertEquals(third.getString(), "hel");

    DataFiled fourth = new StringDataField("hello", 10);
    assertEquals(fourth.getString(), "hello     ");
  }

	@Test
  public void TestStringDataBoxSetters() {
    DataFiled first = new StringDataField();
    assertEquals("", first.getString());

		first.setString("test1234", 8);
    assertEquals("test1234", first.getString());

		first.setString("test1234", 6);
    assertEquals("test12", first.getString());

		first.setString("test1234", 10);
    assertEquals("test1234  ", first.getString());
	}

  @Test
  public void TestStringDataBoxType() {
    DataFiled first = new StringDataField("LOL", 3);
    assertEquals(DataFiled.Types.STRING, first.type());
  }

  @Test
  public void TestStringDataBoxEquals() {
    DataFiled first = new StringDataField("1234", 4);
    DataFiled second = new StringDataField("1234",4);
    assertEquals(first, second);
  }

  @Test
  public void TestStringDataBoxCompare() {
    DataFiled first = new StringDataField("ABCC", 4);
    DataFiled second = new StringDataField("ABCD", 4);
    assertTrue(first.compareTo(second) == -1);
    first.setString("ABCD", 4);
    assertTrue(first.compareTo(second) == 0);
    first.setString("ABCE", 4);
    assertTrue(first.compareTo(second) == 1);
  }

  @Test
  public void TestStringDataBoxSerialize() {
		String testString = "Test Serialize";
    DataFiled first = new StringDataField(testString, testString.length());
    byte[] b = first.getBytes();
    assertEquals(b.length, testString.length());
    assertEquals(b.length, first.getSize());
    DataFiled sec = new StringDataField(b);
    assertEquals(first, sec);
    assertEquals(testString, sec.getString());
  }

  @Test
  public void TestStringDataBoxSerialize2() {
		String testString = "Test Serialize";
    DataFiled first = new StringDataField(testString, testString.length() + 10);
    byte[] b = first.getBytes();
    assertEquals(b.length, testString.length() + 10);
    assertEquals(b.length, first.getSize());
    DataFiled sec = new StringDataField(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataFiledException.class)
  public void TestStringDataBoxString() {
    DataFiled first = new StringDataField("hello", 3);
    first.getInt();
  }

  @Test(expected = DataFiledException.class)
  public void TestStringDataBoxString2() {
    DataFiled first = new StringDataField("test ", 5);
    first.setFloat(89.9f);
  }
}
