package org.wing4j.rrd;

import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.util.Properties;

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
    String workPath = "target";
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

    public RoundRobinConfig() {
        Properties config = new Properties();
        FileInputStream fis = null;
        try {
            File file = new File(new File(workPath), "etc" + File.separator + "config.properties");
            if (!file.exists()) {
                File etcDir = file.getParentFile();
                if (!etcDir.exists()) {
                    etcDir.mkdirs();
                }
                file.createNewFile();
            } else {
                fis = new FileInputStream(file);
                config.load(fis);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        {
            String key = "org.wing4j.rrd.database.autoPersistent";
            if (config.containsKey(key)) {
                autoPersistent = Boolean.valueOf(config.getProperty(key));
            }
        }
        {
            String key = "org.wing4j.rrd.database.asyncPersistent";
            if (config.containsKey(key)) {
                asyncPersistent = Boolean.valueOf(config.getProperty(key));
            }
        }
        {
            String key = "org.wing4j.rrd.database.workPath";
            if (config.containsKey(key)) {
                workPath = config.getProperty(key);
            }
        }
        {
            String key = "org.wing4j.rrd.database.autoPersistentPeriodSec";
            if (config.containsKey(key)) {
                autoPersistentPeriodSec = Integer.valueOf(config.getProperty(key));
            }
        }
        {
            String key = "org.wing4j.rrd.database.autoDisconnectThreshold";
            if (config.containsKey(key)) {
                autoDisconnectThreshold = Integer.valueOf(config.getProperty(key));
            }
        }
        {
            String key = "org.wing4j.rrd.database.autoPersistentRecordThreshold";
            if (config.containsKey(key)) {
                autoPersistentRecordThreshold = Integer.valueOf(config.getProperty(key));
            }
        }
        {
            String key = "org.wing4j.rrd.database.connectorType";
            if (config.containsKey(key)) {
                connectorType = ConnectorType.valueOf(config.getProperty(key));
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Properties config = new Properties();
                config.setProperty("org.wing4j.rrd.database.autoPersistent", Boolean.toString(RoundRobinConfig.this.isAutoPersistent()));
                config.setProperty("org.wing4j.rrd.database.asyncPersistent", Boolean.toString(RoundRobinConfig.this.isAsyncPersistent()));
                config.setProperty("org.wing4j.rrd.database.workPath", RoundRobinConfig.this.getWorkPath());
                config.setProperty("org.wing4j.rrd.database.autoPersistentPeriodSec", Integer.toString(RoundRobinConfig.this.getAutoPersistentPeriodSec()));
                config.setProperty("org.wing4j.rrd.database.autoDisconnectThreshold", Integer.toString(RoundRobinConfig.this.getAutoDisconnectThreshold()));
                config.setProperty("org.wing4j.rrd.database.autoPersistentRecordThreshold", Integer.toString(RoundRobinConfig.this.getAutoPersistentRecordThreshold()));
                config.setProperty("org.wing4j.rrd.database.connectorType", RoundRobinConfig.this.getConnectorType().name());
                FileOutputStream fos = null;
                try {
                    File file = new File(new File(workPath), "etc" + File.separator + "config.properties");
                    if (!file.exists()) {
                        File etcDir = file.getParentFile();
                        if (!etcDir.exists()) {
                            etcDir.mkdirs();
                        }
                    }
                    fos = new FileOutputStream(file);
                    config.store(fos, "Round-Robin-Database config");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
    }
}
