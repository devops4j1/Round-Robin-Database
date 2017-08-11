package org.wing4j.rrd.server;

/**
 * Created by wing4j on 2017/8/10.
 */
public interface ServerInterceptor {
    boolean accept();
    void handle();
}
