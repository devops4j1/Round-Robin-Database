package org.wing4j.rrd.core;

import org.junit.Test;
import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.RoundRobinConnection;


/**
 * Created by wing4j on 2017/8/11.
 */
public class RemoteRoundRobinDatabaseTest {

    @Test
    public void testGetConnection() throws Exception {

    }

    @Test
    public void testOpen() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        config.setRrdHome("./target/x");
        RoundRobinDatabaseRemote database = RemoteRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open("127.0.0.1", 8099, "admin", "password");
        connection.close();
    }

    @Test
    public void testIncrease() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        config.setRrdHome("./target/x");
        RoundRobinDatabaseRemote database = RemoteRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open("127.0.0.1", 8099, "admin", "password");
        for (int i = 0; i < 100; i++) {
            long val = connection.increase("test1", "request", 1, 1);
            System.out.println(val);
        }
    }

    @Test
    public void testOpen1() throws Exception {

    }

    @Test
    public void testGetTable() throws Exception {

    }

    @Test
    public void testCreateTable() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        config.setRrdHome("./target/x");
        RoundRobinDatabaseRemote database = RemoteRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open("127.0.0.1", 8099, "admin", "password");
//        for (int i = 0; i < 100; i++) {
//            long val = connection.increase("test1", "request", 0, 1);
//            System.out.println(val);
//        }
        connection.createTable("test1", "request");
    }

}