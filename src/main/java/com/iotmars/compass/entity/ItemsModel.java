package com.iotmars.compass.entity;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author CJ
 * @date: 2023/4/1 9:49
 */
public class ItemsModel implements Comparable<ItemsModel> {
    private String deviceType;
    private String iotId;
    private String requestId;
    private String checkFailedData;
    private String productKey;
    private Long gmtCreate;
    private String deviceName;
    private JsonNode items;

    public ItemsModel() {
    }

    public ItemsModel(String deviceType, String iotId, String requestId, String checkFailedData, String productKey, Long gmtCreate, String deviceName, JsonNode items) {
        this.deviceType = deviceType;
        this.iotId = iotId;
        this.requestId = requestId;
        this.checkFailedData = checkFailedData;
        this.productKey = productKey;
        this.gmtCreate = gmtCreate;
        this.deviceName = deviceName;
        this.items = items;
    }

    @Override
    public int compareTo(ItemsModel itemsModel) {
//        items.get("value")

        return 0;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getIotId() {
        return iotId;
    }

    public void setIotId(String iotId) {
        this.iotId = iotId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getCheckFailedData() {
        return checkFailedData;
    }

    public void setCheckFailedData(String checkFailedData) {
        this.checkFailedData = checkFailedData;
    }

    public String getProductKey() {
        return productKey;
    }

    public void setProductKey(String productKey) {
        this.productKey = productKey;
    }

    public Long getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Long gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public JsonNode getItems() {
        return items;
    }

    public void setItems(JsonNode items) {
        this.items = items;
    }

    @Override
    public String toString() {
        return "ItemsModel{" +
                "deviceType='" + deviceType + '\'' +
                ", iotId='" + iotId + '\'' +
                ", requestId='" + requestId + '\'' +
                ", checkFailedData='" + checkFailedData + '\'' +
                ", productKey='" + productKey + '\'' +
                ", gmtCreate=" + gmtCreate +
                ", deviceName='" + deviceName + '\'' +
                ", items=" + items +
                '}';
    }
}
