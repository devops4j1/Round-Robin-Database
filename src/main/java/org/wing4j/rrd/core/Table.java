package org.wing4j.rrd.core;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinResultSet;
import org.wing4j.rrd.RoundRobinView;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/3.
 */
public interface Table {
    /**
     * 获取表元信息
     * @return
     */
    TableMetadata getMetadata();

    /**
     * 加锁
     * @return
     */
    Table lock();

    /**
     * 解锁
     * @return
     */
    Table unlock();

    /**
     * 自增
     * @param column
     * @return
     */
    Table increase(String column);

    /**
     * 自增
     * @param column
     * @param i
     * @return
     */
    Table increase(String column, int i);

    /**
     * 获取表所有数据
     * @return
     */
    long[][] getData();

    /**
     * 获取记录行数
     * @return
     */
    long getSize();

    /**
     * 对表进行切片
     * @param size 切片长度
     * @param columns 切片的字段
     * @return 视图切片
     */
    RoundRobinView slice(int size, String... columns);

    /**
     * 读取数据
     * @param columns 字段
     * @return 结果集
     */
    RoundRobinResultSet read(String... columns);

    /**
     * 增加字段，扩容
     * @param columns
     * @return
     */
    Table expand(String... columns);

    /**
     * 合并视图切片
     * @param view
     * @param time
     * @param mergeType
     * @return
     */
    Table merge(RoundRobinView view, int time, MergeType mergeType);

    /**
     * 持久化
     * @param formatType
     * @param version
     * @return
     * @throws IOException
     */
    Table persistent(FormatType formatType, int version) throws IOException;

    /**
     * 持久化
     * @return
     * @throws IOException
     */
    Table persistent() throws IOException;
}
