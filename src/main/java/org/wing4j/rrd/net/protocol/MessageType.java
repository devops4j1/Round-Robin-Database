package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/5.
 */
public enum  MessageType {
    REQUEST(1, "请求"),
    RESPONSE(2, "应答");
    int code;
    String desc;

    MessageType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static MessageType valueOfCode(int code){
        MessageType[] values = values();
        for (MessageType value : values){
            if(value.code == code){
                return value;
            }
        }
        return REQUEST;
    }
}
