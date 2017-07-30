package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/30.
 */
public interface RoundRobinTrigger {
    String getName();
    boolean accept(int time, long data);
    /**
     * 触发器，
     * @param time 时间
     * @param data 数据
     */
    void trigger(int time, long data);
}
