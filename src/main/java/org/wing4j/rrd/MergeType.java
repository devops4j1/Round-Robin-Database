package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/30.
 */
public enum MergeType {
    REP(1, "替换"),
    ADD(2, "加"),
    SUB(3, "减"),
    AVG(4, "均值");
    int code;
    String desc;

    MergeType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static MergeType valueOfCode(int code){
        MergeType[] values = values();
        for (MergeType value : values){
            if(value.code == code){
                return value;
            }
        }
        return REP;
    }
}
