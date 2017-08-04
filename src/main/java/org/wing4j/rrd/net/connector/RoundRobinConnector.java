package org.wing4j.rrd.net.connector;

import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;

import java.io.IOException;

/**
 * Created by wing4j on 2017/7/31.
 * 连接器
 */
public interface RoundRobinConnector {
    /**
     * 通信地址
     *
     * @return
     */
    String getAddress();

    /**
     * 端口号
     *
     * @return
     */
    int getPort();

    /**
     * 获取表对象
     *
     * @param tableName 表名
     * @return 表对象
     * @throws IOException
     */
    TableMetadata getTableMetadata(String tableName) throws IOException;

    /**
     * 获取环状表数据条数
     * @param tableName 表名
     * @return 记录条数
     * @throws IOException
     */
    int getDataSize(String tableName) throws IOException;

    /**
     * 自增
     *
     * @param tableName 表名
     * @param column 字段名
     * @param i 自增量
     * @return 自增后的值
     * @throws IOException
     */
    long increase(String tableName, String column, int i) throws IOException;

    /**
     * 读取数据
     * @param size 读取数据长度
     * @param tableName  表名
     * @param columns 字段名
     * @return 视图切片
     * @throws IOException
     */
    RoundRobinView read(int size, String tableName, String... columns) throws IOException;

    /**
     * 读取数据
     * @param pos 读取结束的偏移位置
     * @param size 读取数据长度
     * @param tableName  表名
     * @param columns 字段名
     * @return 视图切片
     * @throws IOException
     */
    RoundRobinView read(int pos, int size, String tableName, String... columns) throws IOException;

    /**
     * 写入数据
     * @param tableName 表名
     * @param pos 写入结束的偏移位置
     * @param view 视图切片
     * @param mergeType 合并类型
     * @return
     * @throws IOException
     */
    RoundRobinConnector merge(String tableName, int pos, RoundRobinView view, MergeType mergeType) throws IOException;
    /**
     * 写入数据
     * @param tableName 表名
     * @param view 视图切片
     * @param mergeType 合并类型
     * @return
     * @throws IOException
     */
    RoundRobinConnector merge(String tableName, RoundRobinView view, MergeType mergeType) throws IOException;

    /**
     * 字段增加
     * @param tableName 表名
     * @param columns 增加的字段数组
     * @return
     * @throws IOException
     */
    RoundRobinConnector expand(String tableName, String... columns) throws IOException;

    /**
     * 创建表结构
     *
     * @param tableName 表名
     * @param columns   字段数组
     * @return
     */
    RoundRobinConnector createTable(String tableName, String... columns) throws IOException;

    /**
     * 删除表结构
     *
     * @param tableNames 表名数组
     * @return
     */
    RoundRobinConnector dropTable(String... tableNames) throws IOException;
}
