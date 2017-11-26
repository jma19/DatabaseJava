package edu.uci.jun.database.datafiled;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Sammy Sidhu
 * @version 1.0
 */

public class TestBoolDataBox {
    @Test
    public void TestBoolDataBoxConstructor() {
        DataFiled first = new BoolDataFiled(true);
        assertEquals(first.getBool(), true);

        DataFiled sec = new BoolDataFiled(false);
        assertEquals(sec.getBool(), false);
    }

    @Test
    public void TestBoolDataBoxSetters() {
        DataFiled first = new BoolDataFiled();
        first.setBool(true);
        assertEquals(first.getBool(), true);
        first.setBool(false);
        assertEquals(first.getBool(), false);
    }

    @Test
    public void TestBoolDataBoxType() {
        DataFiled first = new BoolDataFiled(true);
        assertEquals(first.type(), DataFiled.Types.BOOL);
    }

    @Test
    public void TestBoolDataBoxEquals() {
        DataFiled first = new BoolDataFiled(false);
        DataFiled second = new BoolDataFiled(false);
        assertEquals(first, second);
    }

    @Test
    public void TestBoolDataBoxCompare() {
        DataFiled first = new BoolDataFiled(false);
        DataFiled second = new BoolDataFiled(true);
        assertTrue(first.compareTo(second) == -1);
        first.setBool(true);
        assertTrue(first.compareTo(second) == 0);
    }

    @Test
    public void TestBoolDataBoxSerialize() {
        DataFiled first = new BoolDataFiled(true);
        byte[] b = first.getBytes();
        assertEquals(b.length, 1);
        assertEquals(b.length, first.getSize());
        DataFiled sec = new BoolDataFiled(b);
        assertEquals(first, sec);
    }

    @Test
    public void TestBoolDataBoxSerialize2() {
        DataFiled first = new BoolDataFiled(false);
        byte[] b = first.getBytes();
        assertEquals(b.length, 1);
        assertEquals(b.length, first.getSize());
        DataFiled sec = new BoolDataFiled(b);
        assertEquals(first, sec);
    }

    @Test(expected = DataFiledException.class)
    public void TestBoolDataBoxString() {
        DataFiled first = new BoolDataFiled(true);
        first.getString();
    }

    @Test(expected = DataFiledException.class)
    public void TestBoolDataBoxString2() {
        DataFiled first = new BoolDataFiled(true);
        String s = "LOL";
        first.setString(s, s.length());
    }

    @Test(expected = DataFiledException.class)
    public void TestBoolDataBoxFloat() {
        DataFiled first = new BoolDataFiled(false);
        first.getFloat();
    }

    @Test(expected = DataFiledException.class)
    public void TestBoolDataBoxFloat2() {
        DataFiled first = new BoolDataFiled(true);
        first.setFloat(1.1f);
    }
}
