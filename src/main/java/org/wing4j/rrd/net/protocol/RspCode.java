package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/5.
 */
public enum RspCode {
    SUCCESS((short)200, "成功"),
    FAIL((short)400, "失败");
    short code;
    String desc;

    RspCode(short code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public short getCode() {
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
