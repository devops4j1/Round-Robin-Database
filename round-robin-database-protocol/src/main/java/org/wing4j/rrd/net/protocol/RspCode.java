package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/5.
 */
public enum RspCode {
    SUCCESS(200, "成功"),
    FAIL(400, "失败");
    int code;
    String desc;

    RspCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static RspCode valueOfCode(int code){
        RspCode[] values = values();
        for (RspCode value : values){
            if(value.code == code){
                return value;
            }
        }
        return FAIL;
    }
}
