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
        RoundRobinView view = connection.slice("mo9", 60, "request");
        view.set(0, "request", 1024);
        view.setTime(80);
        RoundRobinConnection remoteConnection =  database.open("127.0.0.1", 8099);
        remoteConnection.merge("ssss", MergeType.ADD, view);
        Thread.sleep(1000);
    }
}