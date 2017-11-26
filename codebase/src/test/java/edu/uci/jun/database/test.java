package edu.uci.jun.database;

import org.junit.Test;

/**
 * Created by junm5 on 2/19/17.
 */
public class test {
    @Test
    public void testName() throws Exception {
        String str = "BOOL:1|INT:4|STRING:3|FLOAT:4|";
        String[] split = str.split("\\|");
        for(String ele : split){
            System.out.println(ele);
        }

    }
}
