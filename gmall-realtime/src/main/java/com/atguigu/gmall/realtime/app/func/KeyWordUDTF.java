package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.MyKeyWordUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {

    public void eval(String text) {

        for (String s : MyKeyWordUtils.analyze(text)) {
            collect(Row.of(s, s.length()));
        }
    }
}
