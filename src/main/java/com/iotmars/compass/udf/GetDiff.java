package com.iotmars.compass.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.util.Objects;
import java.util.Set;

/**
 * @authorflink implicit type conversion between  CJ
 * @date: 2022/10/29 14:52
 */
@FunctionHint(output = @DataTypeHint("ROW<event_name STRING, event_value STRING, event_ori_value STRING>"))
public class GetDiff extends TableFunction<Row> {
    public void eval(String newData, String oldData) {
//        Logger.getLogger("CompareAndGetDiff").info("getDiff 开始解析: " + newData + "  " + oldData);
        if (Objects.isNull(oldData)) {
            // 如果是ErrorCode，为了展示当前异常信息，而不是改变之后才展示，需要添加当前设备的状态
            JSONObject newDataObj = JSON.parseObject(newData);
            JSONObject errorCodeObject = newDataObj.getJSONObject("ErrorCode");
            String errorCode = "";
            if (Objects.isNull(errorCodeObject)) {
                errorCode = "0";
            } else {
                errorCode = errorCodeObject.getString("value");
            }

            collect(Row.of("ErrorCode", "ErrorCode" + ":" + errorCode, "ErrorCode" + ":"));
            return;
        }
        try {
            JSONObject newDataObj = JSON.parseObject(newData);
            JSONObject oldDataObj = JSON.parseObject(oldData);

            Set<String> keys = newDataObj.keySet();

            for (String key : keys) {
                // 获取每个属性的值
                String newValue = newDataObj.getJSONObject(key).getString("value");
                JSONObject oldDataJsonObject = oldDataObj.getJSONObject(key);
                if (Objects.isNull(oldDataJsonObject)) {
                    return;
                }
                String oldValue = oldDataJsonObject.getString("value");

                // 比较属性获取操作事件
                if (!Objects.isNull(newValue) && !newValue.equalsIgnoreCase(oldValue)) {
                    // 菜谱和工作模式有联动，后续有需求要多条件判断
                    if ("CookbookID".equals(key) || "MultiStageName".equals(key) || "StOvMode".equals(key) || "LStOvMode".equals(key) || "RStOvState".equals(key)) {
                        String newV = (newDataObj.containsKey("CookbookID") ? "," + "CookbookID:" + newDataObj.getJSONObject("CookbookID").getString("value") : "")
                                + (newDataObj.containsKey("MultiStageName") ? "," + "MultiStageName:" + newDataObj.getJSONObject("MultiStageName").getString("value") : "")
                                + (newDataObj.containsKey("StOvMode") ? "," + "StOvMode:" + newDataObj.getJSONObject("StOvMode").getString("value") : "")
                                + (newDataObj.containsKey("LStOvMode") ? "," + "LStOvMode:" + newDataObj.getJSONObject("LStOvMode").getString("value") : "")
                                + (newDataObj.containsKey("RStOvState") ? "," + "RStOvState:" + newDataObj.getJSONObject("RStOvState").getString("value") : "");
                        String oldV = (oldDataObj.containsKey("CookbookID") ? "," + "CookbookID:" + oldDataObj.getJSONObject("CookbookID").getString("value") : "")
                                + (oldDataObj.containsKey("MultiStageName") ? "," + "MultiStageName:" + oldDataObj.getJSONObject("MultiStageName").getString("value") : "")
                                + (oldDataObj.containsKey("StOvMode") ? "," + "StOvMode:" + oldDataObj.getJSONObject("StOvMode").getString("value") : "")
                                + (oldDataObj.containsKey("LStOvMode") ? "," + "LStOvMode:" + oldDataObj.getJSONObject("LStOvMode").getString("value") : "")
                                + (oldDataObj.containsKey("LStOvMode") ? "," + "LStOvMode:" + oldDataObj.getJSONObject("LStOvMode").getString("value") : "");
//                        String[] result = {key, newV, oldV};
                        collect(Row.of(key, newV, oldV));
                    } else {
//                        String[] result = {key, key + ":" + newValue, key + ":" + oldValue};
                        collect(Row.of(key, key + ":" + newValue, key + ":" + oldValue));
                    }
//                    System.out.println("getDiff 开始解析: " + newData + "  " + oldData);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Logger.getLogger("CompareAndGetDiff").warn("FastJson解析失败: " + newData + "  " + oldData);
        }
    }
}