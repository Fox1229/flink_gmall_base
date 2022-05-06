package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

/**
 * UV过滤
 */
public class UniqueVisitorFilterFunction extends RichFilterFunction<JSONObject> {

    // mid上一次登录时间
    private ValueState<String> lastVisitDateState;
    private SimpleDateFormat sdf;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDate", Types.STRING);
        // 设置状态生命周期
        valueStateDescriptor.enableTimeToLive(
                StateTtlConfig
                        .newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build()
        );
        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

        sdf = new SimpleDateFormat("yyyyMMdd");
    }

    @Override
    public boolean filter(JSONObject jsonObject) throws Exception {

        String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
        // 如果当前页面是从其它页面跳转过来的，说明这次访问就不属于独立访客了，直接过滤掉
        if (lastPage != null && lastPage.length() > 0) {
            return false;
        }

        // 如果今日存在登录状态，则过滤
        String lastDate = lastVisitDateState.value();
        // 获取设备访问时间
        String currentDate = sdf.format(jsonObject.getLong("ts"));
        if (lastDate != null && lastDate.length() > 0 && lastDate.equals(currentDate)) {
            // 设备在今日访问过
            return false;
        } else {
            // 设备没有在今日访问过
            // 更新状态
            lastVisitDateState.update(currentDate);
            return true;
        }
    }
}
