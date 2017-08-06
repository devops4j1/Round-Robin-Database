package org.wing4j.rrd;

import lombok.Data;
import lombok.ToString;

/**
 * Created by wing4j on 2017/7/30.
 * 配置对象
 */
@Data
@ToString
public class RoundRobinConfig {
    /**
     * 是否自动持久化
     */
    boolean autoPersistent = false;
    /**
     * 是否异步持久化
     */
    boolean asyncPersistent = false;
    /**
     * 工作路径
     */
    String workPath = ".\\target";
    /**
     * 自动化持久频率（单位秒）
     */
    int autoPersistentPeriodSec = 120;
    /**
     * 自动断开连接阈值
     */
    int autoDisconnectThreshold = 60;
    /**
     * 自动持久记录数阈值
     */
    int autoPersistentRecordThreshold = 60;
    /**
     * 连接器类型
     */
    ConnectorType connectorType = ConnectorType.AIO;
}
