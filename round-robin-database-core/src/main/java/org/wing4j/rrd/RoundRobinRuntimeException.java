package org.wing4j.rrd;

/**
 * Created by wing4j on 2017/7/31.
 */
public class RoundRobinRuntimeException extends RuntimeException{
    public RoundRobinRuntimeException(String message) {
        super(message);
    }

    public RoundRobinRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
