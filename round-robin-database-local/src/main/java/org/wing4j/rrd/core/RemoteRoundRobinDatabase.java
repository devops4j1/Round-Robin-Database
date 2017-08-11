package org.wing4j.rrd.core;

import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.net.ConnectorType;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wing4j on 2017/8/11.
 */
public class RemoteRoundRobinDatabase extends DefaultRoundRobinDatabase implements RoundRobinDatabaseRemote {
    //数据库实例
    final static Map<String, RoundRobinDatabaseRemote> instances = new ConcurrentHashMap();

    protected RemoteRoundRobinDatabase(String instance, RoundRobinConfig config) throws IOException {
        super(instance, config);
    }

    public static RoundRobinDatabaseRemote init(RoundRobinConfig config) throws IOException {
        return init("default", config);
    }

    public static RoundRobinDatabaseRemote init(String instance, RoundRobinConfig config) throws IOException {
        RoundRobinDatabaseRemote database = instances.get(instance);
        if (database == null) {
            synchronized (instances) {
                database = instances.get(instance);
                if (database == null) {
                    database = new RemoteRoundRobinDatabase(instance, config);
                }
            }
        }
        return database;
    }

    @Override
    public RoundRobinConnection open(String address, int port, String username, String password) throws IOException {
        ConnectorType connectorType = config.getConnectorType();
        RoundRobinConnector connector = null;
        if(connectorType == ConnectorType.BIO){
           try {
               Class connectorClass = Class.forName("org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector");
               //RoundRobinDatabase database, RoundRobinConfig config, String address, int port
               Constructor constructor = connectorClass.getConstructor(RoundRobinDatabase.class, RoundRobinConfig.class, String.class, Integer.TYPE);
               connector = (RoundRobinConnector)constructor.newInstance(this, config, address, port);
           }catch (Exception e){

           }
        }
        RoundRobinConnection connection = new RemoteRoundRobinConnection(this, connector, username, password);
        connections.put(connection.getSessionId(), connection);
        return connection;
    }
}
