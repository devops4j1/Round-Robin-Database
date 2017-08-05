package org.wing4j.rrd.core;

/**
 * Created by wing4j on 2017/8/4.
 */
public enum TableStatus {
    NORMAL(1, "正常"),
    DROP(2, "删除表结构"),
    LOCK(3, "表锁定"),
    UNKNOWN(0, "未知状态");
    int code;
    String desc;

    TableStatus(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static TableStatus valueOfCode(int code){
        TableStatus[] values = values();
        for (TableStatus value : values){
            if(value.code == code){
                return value;
            }
        }
        return UNKNOWN;
    }
}
