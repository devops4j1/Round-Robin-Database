package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/31.
 */
public enum FormatType {
    BIN(1, "二进制文件"),
    CSV(2, "CSV文件");
    int code;
    String desc;

    FormatType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static FormatType valueOfCode(int code){
        FormatType[] values = values();
        for (FormatType value : values){
            if(value.code == code){
                return value;
            }
        }
        return BIN;
    }
}
