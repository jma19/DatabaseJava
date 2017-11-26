package edu.uci.jun.database;

import com.google.common.collect.Lists;
import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.datafiled.IntDataField;
import edu.uci.jun.database.datafiled.StringDataField;
import edu.uci.jun.database.table.Schema;

import java.util.List;
import java.util.Random;

/**
 * Created by junm5 on 11/25/17.
 */
public class TableCreateUtils {
    private static Random random = new Random();

    public static Schema getSchema() {
        return new Schema("student", Lists.newArrayList("name", "id", "age"),
                Lists.newArrayList(new StringDataField(50), new IntDataField(), new IntDataField()));
    }

    public static List<DataFiled> generateRandomTuple() {
        int length = 10;
        StringBuffer name = new StringBuffer();
        for (int i = 0; i < length; i++) {
            char ch = (char) ('a' + random.nextInt(25));
            name.append(ch);
        }
        return Lists.newArrayList(new StringDataField(name.toString()), new IntDataField(random.nextInt(100)), new IntDataField(24));
    }


}
