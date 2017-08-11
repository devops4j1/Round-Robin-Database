package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/30.
 */
public interface Status {
    /**
     * 0:正常
     */
    int NORMAL = 0;
    /**
     * 1:冻结,数据只能通过合并操作写入
     */
    int FREEZEN = 0;
}
