package com.iotmars.compass.entity;

/**
 * @author CJ
 * @date: 2023/4/1 10:31
 */
public class ItemsModelEventDTO implements Comparable<ItemsModelEventDTO> {
    private String deviceType;
    private String iotId;
    private String requestId;
    private String checkFailedData;
    private String productKey;
    // 这个gmtCreate取的是事件真实发生的时间而，为了避免影响后面的任务还是取名为gmtCreate
    private Long gmtCreate;
    private String deviceName;
    private String eventName;
    private String eventValue;
    private String eventOriValue;

    public ItemsModelEventDTO() {
    }

    public ItemsModelEventDTO(String deviceType, String iotId, String requestId, String checkFailedData, String productKey, Long gmtCreate, String deviceName, String eventName, String eventValue, String eventOriValue) {
        this.deviceType = deviceType;
        this.iotId = iotId;
        this.requestId = requestId;
        this.checkFailedData = checkFailedData;
        this.productKey = productKey;
        this.gmtCreate = gmtCreate;
        this.deviceName = deviceName;
        this.eventName = eventName;
        this.eventValue = eventValue;
        this.eventOriValue = eventOriValue;
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

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventValue() {
        return eventValue;
    }

    public void setEventValue(String eventValue) {
        this.eventValue = eventValue;
    }

    public String getEventOriValue() {
        return eventOriValue;
    }

    public void setEventOriValue(String eventOriValue) {
        this.eventOriValue = eventOriValue;
    }

    @Override
    public String toString() {
        return "ItemsModelEvent{" +
                "deviceType='" + deviceType + '\'' +
                ", iotId='" + iotId + '\'' +
                ", requestId='" + requestId + '\'' +
                ", checkFailedData='" + checkFailedData + '\'' +
                ", productKey='" + productKey + '\'' +
                ", gmtCreate=" + gmtCreate +
                ", deviceName='" + deviceName + '\'' +
                ", eventName='" + eventName + '\'' +
                ", eventValue='" + eventValue + '\'' +
                ", eventOriValue='" + eventOriValue + '\'' +
                '}';
    }

    @Override
    public int compareTo(ItemsModelEventDTO o) {
        return gmtCreate.compareTo(o.gmtCreate);
    }
}
