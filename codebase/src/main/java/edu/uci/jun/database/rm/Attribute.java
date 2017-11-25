package edu.uci.jun.database.rm;

import edu.uci.jun.database.datafiled.DataFiled;

/**
 * Created by junm5 on 11/24/17.
 */
public class Attribute {
    private String name;
    private DataFiled.Types type;
    private Integer length;

    public Attribute(String name, DataFiled.Types type, Integer length) {
        this.name = name;
        this.type = type;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataFiled.Types getType() {
        return type;
    }

    public void setType(DataFiled.Types type) {
        this.type = type;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", length=" + length +
                '}';
    }
}
