package edu.uci.jun.database.rbf;

import edu.uci.jun.database.datafiled.DataFiled;

import java.util.List;
import java.lang.StringBuilder;

/**
 * A wrapper class for an individual record. Simply stores a list of DataBoxes.
 */
public class Record {
    private List<DataFiled> values;

    public Record(List<DataFiled> values) {
        this.values = values;
    }

    public List<DataFiled> getValues() {
        return this.values;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Record)) {
            return false;
        }
        Record otherRecord = (Record) other;
        if (values.size() != otherRecord.values.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!(values.get(i).equals(otherRecord.values.get(i)))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (DataFiled d : values) {
            s.append(d.toString().trim());
            s.append(", ");
        }
        return s.substring(0, s.length() - 2);
    }
}
