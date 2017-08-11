package org.wing4j.rrd.server.authority.impl;

import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.net.authority.AuthorityService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by wing4j on 2017/8/6.
 */
public class SimpleAuthorityService implements AuthorityService {
    RoundRobinServerConfig serverConfig;
    Properties authorities = null;
    long lastLoadTime = System.currentTimeMillis();

    public SimpleAuthorityService(RoundRobinServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public boolean hasAuthority(String username, String password) {
        if(authorities == null){
            synchronized (this){
                if(authorities == null){
                    authorities = new Properties();
                    loadConfig(authorities);
                }
            }
        }
        if(System.currentTimeMillis() - lastLoadTime > 60 * 1000){
            synchronized (this) {
                if(System.currentTimeMillis() - lastLoadTime > 60 * 1000){
                    authorities = new Properties();
                    loadConfig(authorities);
                }
            }
        }
        return password.equals(authorities.getProperty(username));
    }

    void loadConfig(Properties authorities) {
        FileInputStream fis = null;
        try {
            File file = new File(serverConfig.getRrdHome() + File.separator + "etc" + File.separator + "authorities.properties");
            if(!file.exists()){
                File etcDir = new File(serverConfig.getRrdHome() + File.separator + "etc");
                if(!etcDir.exists()){
                    etcDir.mkdirs();
                }
            }else{
                fis = new FileInputStream(file);
                authorities.load(fis);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fis != null){
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}