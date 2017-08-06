package org.wing4j.rrd.server.authority.impl;

import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.server.authority.AuthorityService;

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

    public SimpleAuthorityService(RoundRobinServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public boolean hasAuthority(String username, String password) {
        Properties authorities = new Properties();
        FileInputStream fis = null;
        try {
            File file = new File(serverConfig.getWorkPath() + File.separator + "etc" + File.separator + "authorities.properties");
            if(!file.exists()){
                File etcDir = new File(serverConfig.getWorkPath() + File.separator + "etc");
                if(!etcDir.exists()){
                    etcDir.mkdirs();
                }
                file.createNewFile();
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
        return password.equals(authorities.getProperty(username));
    }
}