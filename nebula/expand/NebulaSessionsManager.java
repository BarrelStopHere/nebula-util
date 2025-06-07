package com.jwwd.flow.nebula.expand;

import com.google.common.collect.Lists;
import com.jwwd.flow.nebula.config.CustomCondition;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.SessionsManagerConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
@Conditional(CustomCondition.class)
@Configuration
public class NebulaSessionsManager {

    private final Environment env;
    private final Map<String, ExpandSessionManager> sessionsManagerMap = new HashMap<>();
    private final List<HostAddress> hostAddresses;
    private final List<String> spaces;

    @Autowired
    public NebulaSessionsManager(Environment env) {
        this.env = env;

        // 缓存 HostAddress 列表
        String nebulaUrl = Objects.requireNonNull(env.getProperty("nebula.url"))
                .replaceAll("jdbc:nebula://", "");
        String[] connections = nebulaUrl.split(",");
        this.hostAddresses = new ArrayList<>();
        for (String conn : connections) {
            String[] parts = conn.split(":");
            hostAddresses.add(new HostAddress(parts[0], Integer.parseInt(parts[1])));
        }
        String spaceProperty = env.getProperty("nebula.spaces");
        assert spaceProperty != null;
        String[] spaceArray = spaceProperty.split(",");
        this.spaces = Arrays.asList(spaceArray);

    }

    @PostConstruct
    public void init() {
        for (String space : spaces) {
            sessionsManagerMap.putIfAbsent(space, of(space));
        }
    }

    @PreDestroy
    public void destroy() {
        log.info("Closing Nebula sessions managers...");
        sessionsManagerMap.values().forEach(manager -> {
            try {
                manager.close(env.getProperty("nebula.username"));
            } catch (Exception e) {
                log.error("关闭连接池失败", e);
            }
        });
    }

    @Bean
    public Map<String, ExpandSessionManager> nebulaSessionsManagers() {
        return sessionsManagerMap;
    }

    private ExpandSessionManager of(String space) {
        NebulaPoolConfig poolConfig = new NebulaPoolConfig();
        poolConfig.setMaxConnSize(100);

        SessionsManagerConfig managerConfig = new SessionsManagerConfig();
        managerConfig.setAddresses(Lists.newArrayList(getHostAddress()));
        managerConfig.setUserName(env.getProperty("nebula.username"));
        managerConfig.setPassword(env.getProperty("nebula.password"));
        managerConfig.setSpaceName(space);
        managerConfig.setReconnect(true);
        managerConfig.setPoolConfig(poolConfig);

        return new ExpandSessionManager(managerConfig);
    }

    /**
     * 简易负载均衡
     */
    private HostAddress getHostAddress() {
        int pos = ThreadLocalRandom.current().nextInt(hostAddresses.size());
        return hostAddresses.get(pos);
    }
}
