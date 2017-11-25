package edu.uci.jun.database.table;

import edu.uci.jun.database.datafiled.*;
import edu.uci.jun.database.rbf.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Schema of a particular table.
 * <p>
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
    private String tableName;
    private List<String> fields;
    private List<DataFiled> fieldTypes;
    private int size;

    public Schema(String tableName, List<String> fields, List<DataFiled> fieldTypes) {
        assert (fields.size() == fieldTypes.size());
        this.tableName = tableName;
        this.fields = fields;
        this.fieldTypes = fieldTypes;
        this.size = 0;

        for (DataFiled dt : fieldTypes) {
            this.size += dt.getSize();
        }
    }

    /**
     *
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Verifies that a list of DataBoxes corresponds to this schema. A list of
     * DataBoxes corresponds to this schema if the number of DataBoxes in the
     * list equals the number of columns in this schema, and if each DataFiled has
     * the same type and size as the columns in this schema.
     *
     * @param values the list of values to check
     * @return a new Record with the DataBoxes specified
     * @throws SchemaException if the values specified don't conform to this Schema
     */
    public Record verify(List<DataFiled> values) throws SchemaException {
        // TODO: implement me!
        if (values.size() != fields.size()) {
            throw new SchemaException("the values specified don't conform to this Schema");
        }
        for (int i = 0; i < fieldTypes.size(); i++) {
            DataFiled dataBox = values.get(i);
            if (dataBox.getClass() != fieldTypes.get(i).getClass()) {
                throw new SchemaException("the values specified don't conform to this Schema");
            }
        }
        return new Record(values);
    }

    /**
     * Serializes the provided record into a byte[]. Uses the DataBoxes'
     * serialization methods. A serialized record is represented as the
     * concatenation of each serialized DataFiled. This method assumes that the
     * input record corresponds to this schema.
     *
     * @param record the record to encode
     * @return the encoded record as a byte[]
     */

    public byte[] encode(Record record) {
        List<DataFiled> values = record.getValues();
        //concatenation of each serialized datafiled
        StringBuffer head = new StringBuffer();
        List<Byte> res = new ArrayList();

        for (DataFiled dataBox : values) {
            byte[] bytes = dataBox.getBytes();
            head.append(dataBox.type()).append(":").append(dataBox.getSize()).append("|");
            for (byte by : bytes) {
                res.add(by);
            }
        }
        byte[] headBytes = head.toString().getBytes();
        int headLength = headBytes.length;
        byte[] recordRes = new byte[res.size() + headLength + 4];
        //add head length
        int index = 0;
        for (int i = 0; i < 4; i++) {
            recordRes[index++] = (byte) ((headLength >>> (i * 8)) & 0xff);
        }
        //add head
        for (int i = 0; i < headBytes.length; i++) {
            recordRes[index++] = headBytes[i];
        }
        //add data
        for (int i = 0; i < res.size(); i++) {
            recordRes[index++] = res.get(i);
        }
        return recordRes;
    }

    /**
     * Takes a byte[] and decodes it into a Record. This method assumes that the
     * input byte[] represents a record that corresponds to this schema.
     *
     * @param input the byte array to decode
     * @return the decoded Record
     */
    public Record decode(byte[] input) {
        int start = 0;
        int headLength = getInt(input, start);
        start += 4;
        String headFileds = new String(Arrays.copyOfRange(input, start, start + headLength));
        start += headLength;
        String[] fields = headFileds.split("\\|");
        List<DataFiled> values = new ArrayList();
        for (String field : fields) {
            if (field != null && !field.equals("")) {
                if (field.contains("INT")) {
                    IntDataField intDataBox = new IntDataField(Arrays.copyOfRange(input, start, start + 4));
                    values.add(intDataBox);
                    start += intDataBox.getSize();
                }
                //public enum Types {BOOL, INT, FLOAT, STRING}
                else if (field.contains("BOOL")) {
                    BoolDataFiled boo = new BoolDataFiled(Arrays.copyOfRange(input, start, start + 1));
                    values.add(boo);
                    start += boo.getSize();
                } else if (field.contains("FLOAT")) {
                    FloatDataField floatDataBox = new FloatDataField(Arrays.copyOfRange(input, start, start + 4));
                    values.add(floatDataBox);
                    start += floatDataBox.getSize();
                } else if (field.contains("STRING")) {
                    int indexOfSplit = field.indexOf(":");
                    int lenStr = Integer.valueOf(field.substring(indexOfSplit + 1));
                    StringDataField stringDataBox = new StringDataField(Arrays.copyOfRange(input, start, start + lenStr));
                    values.add(stringDataBox);
                    start += stringDataBox.getSize();
                }
            }
        }

        return new Record(values);

    }

    private int getInt(byte bytes[], int off) {
        int b0 = bytes[off] & 0xFF;
        int b1 = bytes[off + 1] & 0xFF;
        int b2 = bytes[off + 2] & 0xFF;
        int b3 = bytes[off + 3] & 0xFF;
        return b0 | (b1 << 8) | (b2 << 16) | b3 << 24;
    }


    public int getEntrySize() {
        return this.size;
    }

    public List<String> getFieldNames() {
        return this.fields;
    }

    public List<DataFiled> getFieldTypes() {
        return this.fieldTypes;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Schema)) {
            return false;
        }
        Schema otherSchema = (Schema) other;
        if (this.fields.size() != otherSchema.fields.size()) {
            return false;
        }
        for (int i = 0; i < this.fields.size(); i++) {
            DataFiled thisType = this.fieldTypes.get(i);
            DataFiled otherType = otherSchema.fieldTypes.get(i);

            if (thisType.type() != otherType.type()) {
                return false;
            }

            if (thisType.type().equals(DataFiled.Types.STRING) && thisType.getSize() != otherType.getSize()) {
                return false;
            }
        }

        return true;
    }

    public static void main(String[] args) {
        BoolDataFiled dataBox = new BoolDataFiled(true);
        System.out.println(dataBox.type().toString());
    }
}
