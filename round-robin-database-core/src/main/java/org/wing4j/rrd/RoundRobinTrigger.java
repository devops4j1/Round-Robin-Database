package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/30.
 * 监控触发器
 */
public interface RoundRobinTrigger {
    /**
     * 监控的字段
     * @return
     */
    String getName();

    /**
     * 是否符合触发条件
     * @param time 时间
     * @param data 数据
     * @return 触发返回真
     */
    boolean accept(int time, long data);
    /**
     * 触发器，
     * @param time 时间
     * @param data 数据
     */
    void trigger(int time, long data);
}
