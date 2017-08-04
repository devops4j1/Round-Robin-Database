package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/4.
 */
public enum ProtocolType {
    MERGE(1, "合并视图"),
    TABLE_METADATA(2, "获取表元信息"),
    INCREASE(3, "字段自增"),
    SLICE(4, "获取视图切片"),
    QUERY_PAGE(5, "分页查看"),
    EXPAND(6, "字段扩容"),
    CREATE_TABLE(7, "创建表"),
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
