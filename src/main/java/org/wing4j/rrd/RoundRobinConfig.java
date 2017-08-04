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
    String workPath = "./target";
    /**
     * 连接器类型
     */
    ConnectorType connectorType = ConnectorType.BIO;
}
