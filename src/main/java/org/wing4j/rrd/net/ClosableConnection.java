package org.wing4j.rrd.net;

public interface ClosableConnection {
	String getCharset();
	void close(String reason);
	boolean isClosed();
	 void idleCheck();
	long getStartupTime();
	String getHost();
	int getPort();
	int getLocalPort();
	long getNetInBytes();
	long getNetOutBytes();
}