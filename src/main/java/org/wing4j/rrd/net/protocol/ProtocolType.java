package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/4.
 */
public enum ProtocolType {
    MERGE(1, "合并视图"),
    GET_DATA_SIZE(2, "获取数据条数"),
    SLICE(20, "获取视图切片"),
    UNKNOWN(0, "未知命令");
    int code;
    String desc;

    ProtocolType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static ProtocolType valueOfCode(int code){
        ProtocolType[] values = values();
        for (ProtocolType value : values){
            if(value.code == code){
                return value;
            }
        }
        return UNKNOWN;
    }
}