package org.wing4j.rrd.core;

import org.wing4j.rrd.*;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/3.
 */
public interface Table {
    /**
     * 获取表元信息
     *
     * @return
     */
    TableMetadata getMetadata();

    /**
     * 加锁
     *
     * @return
     */
    Table lock();

    /**
     * 解锁
     *
     * @return
     */
    Table unlock();

    /**
     * 自增
     *
     * @param column
     * @return
     */
    long increase(String column);

    /**
     * 自增
     *
     * @param column
     * @param val
     * @return
     */
    long increase(String column, int val);

    long increase(int pos, String column, int val);

    /**
     * 获取记录行数
     *
     * @return
     */
    int getSize();

    /**
     * 设置数据
     * @param pos
     * @param column
     * @param val
     * @return
     */
    long set(int pos, String column, long val);

    /**
     * 获取数据
     * @param pos
     * @param column
     * @return
     */
    long get(int pos, String column);
    /**
     * 对表进行切片
     *
     * @param size    切片长度
     * @param columns 切片的字段
     * @return 视图切片
     */
    RoundRobinView slice(int size, String... columns);

    RoundRobinView slice(int size, int time, String... columns);

    /**
     * 增加字段，扩容
     *
     * @param columns
     * @return
     */
    Table expand(String... columns);

    /**
     * 合并视图切片
     *
     * @param view
     * @param mergePos
     * @param mergeType
     * @return
     */
    RoundRobinView merge(RoundRobinView view, int mergePos, MergeType mergeType);
    RoundRobinView merge(RoundRobinView view, MergeType mergeType);

    /**
     * 持久化
     *
     * @param formatType
     * @param version
     * @return
     * @throws IOException
     */
    Table persistent(FormatType formatType, int version) throws IOException;

    /**
     * 持久化
     *
     * @return
     * @throws IOException
     */
    Table persistent() throws IOException;

    /**
     * 删除表
     * @throws IOException
     */
    void drop() throws IOException;

    /**
     * 增加触发器
     * @param trigger
     * @return
     */
    Table registerTrigger(RoundRobinTrigger trigger);
}
