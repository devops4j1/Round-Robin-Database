package org.wing4j.rrd;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by wing4j on 2017/7/29.
 */
public interface RoundRobinConnection {
    int DAY_SECOND = 24 * 60 * 60;

    /**
     * 冻结数据
     * @return
     */
    RoundRobinConnection freezen();

    /**
     * 解冻
     * @return
     */
    RoundRobinConnection unfreezen();
    /**
     * 获取数据库
     * @return
     */
    RoundRobinDatabase getDatabase();
    /**
     * 获取表头
     * @return
     */
    String[] getHeader();

    /**
     * 读取所有的数据
     * @param name
     * @return
     */
    long[][] read(String... name);
    /**
     * 是否包含头名称
     * @param name 头名称
     * @return
     */
    boolean contain(String name);
    /**
     * 对应表头数据自增1
     * @param sec
     * @param name
     * @return 连接上下文
     */
    RoundRobinConnection increase(int sec,String name);

    /**
     * 对应表头数据自增1
     * @param name
     * @return 连接上下文
     */
    RoundRobinConnection increase(String name);

    /**
     * 对应表头数据自增i
     * @param name
     * @param i 增量
     */
    RoundRobinConnection increase(String name, int i);

    /**
     * 对应表头数据自增i
     * @param sec
     * @param name
     * @param i
     * @return 连接上下文
     */
    RoundRobinConnection increase(int sec,String name, int i);

    /**
     * 数据切片,获取距今size秒之前的切片数据
     * @param second 秒数
     * @param name 头名称数组
     * @return 视图切片
     */
    RoundRobinView slice(int second, String... name);

    /**
     * 增加触发器
     * @param trigger 触发器
     * @return 连接上下文
     */
    RoundRobinConnection addTrigger(RoundRobinTrigger trigger);
    /**
     * 合并切片数据到数据库
     * @param view 切片视图数据
     * @param mergeType 合并类型
     * @return 连接上下文
     */
    RoundRobinConnection merge(RoundRobinView view, MergeType mergeType);

    /**
     * 合并切片数据到数据库
     * @param view 切片视图数据
     * @param time 合并起始时间点
     * @param mergeType 合并类型
     * @return 连接上下文
     */
    RoundRobinConnection merge(RoundRobinView view, int time, MergeType mergeType);
    /**
     * 持久化
     * @param formatType 持久化文件格式
     * @param version 版本号
     * @return 连接上下文
     */
    RoundRobinConnection persistent(FormatType formatType, int version) throws IOException;

    /**
     * 持久化为BIN格式最新版本
     * @return 连接上下文
     * @throws IOException
     */
    RoundRobinConnection persistent() throws IOException;

    /**
     * 关闭连接
     */
    void close() throws IOException;

    /**
     * 转换成流输出
     * @return
     */
    InputStream toStream();
}
