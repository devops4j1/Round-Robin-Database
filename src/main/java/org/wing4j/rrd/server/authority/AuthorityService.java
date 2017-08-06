package org.wing4j.rrd.server.authority;

/**
 * Created by wing4j on 2017/8/6.
 * 权限认证服务
 */
public interface AuthorityService {
    /**
     * 是否有操作权限
     *
     * @param username
     * @param password
     * @return
     */
    boolean hasAuthority(String username, String password);
}
