package com.jwwd.flow.nebula.expand;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jwwd.common.redis.service.RedisService;
import com.jwwd.flow.nebula.config.CustomCondition;
import com.jwwd.flow.nebula.space.Space;
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
    private final Map<Space, ExpandSessionManager> sessionsManagerMap = new HashMap<>();
    private final List<HostAddress> hostAddresses;

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
    }

    @PostConstruct
    public void init() {
        for (Space space : Space.values()) {
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
    public Map<Space, ExpandSessionManager> nebulaSessionsManagers() {
        return sessionsManagerMap;
    }

    private ExpandSessionManager of(Space space) {
        NebulaPoolConfig poolConfig = new NebulaPoolConfig();
        poolConfig.setMaxConnSize(100);

        SessionsManagerConfig managerConfig = new SessionsManagerConfig();
        managerConfig.setAddresses(Lists.newArrayList(getHostAddress()));
        managerConfig.setUserName(env.getProperty("nebula.username"));
        managerConfig.setPassword(env.getProperty("nebula.password"));
        managerConfig.setSpaceName(space.getKey());
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
