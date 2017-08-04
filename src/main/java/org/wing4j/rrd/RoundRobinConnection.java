package org.wing4j.rrd;

import java.io.IOException;
import java.util.Map;

/**
 * Created by wing4j on 2017/7/29.
 */
public interface RoundRobinConnection {
    int DAY_SECOND = 24 * 60 * 60;
    /**
     * 获取数据库
     *
     * @return
     */
    RoundRobinDatabase getDatabase();

    /**
     * 获取表头
     *
     * @return
     */
    String[] getColumns(String tableName);

    /**
     * 是否包含头名称
     *
     * @param tableName 表名
     * @param column 字段名
     * @return
     */
    boolean contain(String tableName, String column);

    /**
     * 对应表头数据自增1
     *
     * @param tableName 表名
     * @param column 字段名
     * @return 连接上下文
     */
    long increase(String tableName, String column);

    /**
     * 对应表头数据自增1
     *
     * @param tableName 表名
     * @param column 字段名
     * @param i 增量
     *
     * @return 连接上下文
     */
    long increase(String tableName, String column, int i);

    /**
     * 数据切片,获取距今size秒之前的切片数据
     *
     * @param tableName 表名
     * @param size      秒数
     * @param columns   字段数组
     * @return 视图切片
     */
    RoundRobinView slice(String tableName, int size, String... columns);

    /**
     * 数据切片,获取距今size秒之前的切片数据
     *
     * @param size      长度
     * @param fullNames 表名.字段名
     * @return
     */
    RoundRobinView slice(int size, String... fullNames);

    /**
     * 增加触发器
     *
     * @param trigger 触发器
     * @return 连接上下文
     */
    RoundRobinConnection registerTrigger(String tableName, RoundRobinTrigger trigger);

    /**
     * 合并切片数据到数据库
     *
     * @param view      切片视图数据
     * @param mergeType 合并类型
     * @return 连接上下文
     */
    RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view);

    /**
     * 合并切片数据到数据库
     * @param tableName 表名
     * @param mergeType 合并类型
     * @param view 视图对象
     * @param mappings 映射关系
     * @return 连接上下文
     */
    RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view, Map<String, String> mappings);

    RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view);
    RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view, Map<String, String> mappings);
    /**
     * 持久化
     *
     * @param formatType 持久化文件格式
     * @param version    版本号
     * @param tableNames    表名数组
     * @return 连接上下文
     */
    RoundRobinConnection persistent(FormatType formatType, int version, String ... tableNames) throws IOException;

    /**
     * 持久化为BIN格式最新版本
     * @param tableNames    表名数组
     * @return 连接上下文
     * @throws IOException
     */
    RoundRobinConnection persistent(String ... tableNames) throws IOException;
    /**
     * 创建表
     * @param tableName 表名
     * @param columns 字段数组
     * @return
     */
    RoundRobinConnection createTable(String tableName, String ... columns) throws IOException;

    /**
     * 删除表
     * @param tableNames
     * @return
     */
    RoundRobinConnection dropTable(String...tableNames) throws IOException;
    /**
     * 关闭连接
     */
    void close() throws IOException;
}
