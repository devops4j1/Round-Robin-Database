package org.wing4j.rrd.client;

import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;

/**
 * Created by wing4j on 2017/8/2.
 */
public class RemoteRoundRobinConnectionTest {

    @Test
    public void testMerge() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open();
        RoundRobinView view = connection.slice("mo9", 1, 1, "request");
        view.set(0, "request", 1024);
        view.setTime(80);
        RoundRobinConnection remoteConnection =  database.open("127.0.0.1", 8099);
        remoteConnection.merge("ssss", MergeType.ADD, view);
        Thread.sleep(1000);
    }

    @Test
    public void testSlice() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection remoteConnection =  database.open("127.0.0.1", 8099);
        RoundRobinView view =  remoteConnection.slice("ssss", 60, 60, "request");
        Thread.sleep(1000);
    }
    @Test
    public void testGetColumns() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection remoteConnection =  database.open("127.0.0.1", 8099);
        remoteConnection.getColumns("ssss");
        Thread.sleep(1000);
    }
}