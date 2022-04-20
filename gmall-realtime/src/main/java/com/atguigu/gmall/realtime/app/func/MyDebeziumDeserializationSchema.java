package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * 反序列化flinkCDC读取的数据
 */
public class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = -31165416854840603L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        Struct struct = (Struct) sourceRecord.value();

        // 获取操作类型
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        // 获取数据库和表名
        Struct source = struct.getStruct("source");
        String database = source.getString("db");
        String table = source.getString("table");
        JSONObject resJsonObj = new JSONObject();
        resJsonObj.put("db", database);
        resJsonObj.put("table", table);
        resJsonObj.put("type", type);

        // 获取表的字段信息
        Struct after = struct.getStruct("after");
        JSONObject dataJsonObj = new JSONObject();
        if (after != null) {
            List<Field> fieldList = after.schema().fields();
            for (Field field : fieldList) {
                // 获取字段名和字段值
                String fieldName = field.name();
                Object fieldsValue = after.get(fieldName);
                dataJsonObj.put(fieldName, fieldsValue);
            }
        }
        resJsonObj.put("data", dataJsonObj);

        collector.collect(resJsonObj.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
