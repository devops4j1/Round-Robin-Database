package org.wing4j.rrd.net.protocol;

/**
 * Created by wing4j on 2017/8/4.
 */
public enum ProtocolType {
    CONNECT(1, "建立连接"),
    DIS_CONNECT(2, "断开连接"),
    MERGE(3, "合并视图"),
    TABLE_METADATA(4, "获取表元信息"),
    INCREASE(5, "字段自增"),
    SLICE(6, "获取视图切片"),
    QUERY_PAGE(7, "分页查看"),
    EXPAND(8, "字段扩容"),
    CREATE_TABLE(9, "创建表"),
    DROP_TABLE(10, "删除表"),
    PERSISTENT_TABLE(11, "持久化表"),
    GET(12, "获取数据"),
    SET(13, "设置数据"),
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
