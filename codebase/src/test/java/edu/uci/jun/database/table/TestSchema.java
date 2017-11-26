package edu.uci.jun.database.table;

import edu.uci.jun.database.TestUtils;

import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.rbf.Record;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class TestSchema {
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataFiled>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataFiled> values = new ArrayList<DataFiled>();

    values.add(new StringDataField("abcde", 5));
    values.add(new IntDataField(10));

    schema.verify(values);
  }

  @Test
  public void test_encode_decode_record() throws Exception {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataFiled> values = new ArrayList<>();
    values.add(new BoolDataFiled(true));
    values.add(new IntDataField(122));
    values.add(new StringDataField("abc", 3));
    values.add(new FloatDataField(4.2f));

    Record record = new Record(values);

    byte[] encode = schema.encode(record);

    Record decode = schema.decode(encode);
    assertThat(record.equals(decode),is(true));
  }


}
