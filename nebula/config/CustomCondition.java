package com.jwwd.flow.nebula.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Slf4j
public class CustomCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        // 通过ConditionContext获取Environment
        Environment env = context.getEnvironment();

        // 直接从Environment中读取配置值
        String enabled = env.getProperty("nebula.enabled");
        String url = env.getProperty("nebula.url");

        // 处理默认值和空值
        boolean isOpen = "true".equalsIgnoreCase(enabled) && enabled != null;
        boolean b = (url != null && !url.trim().isEmpty()) && isOpen;
        log.info("nebula启用状态: {} -> {}", enabled, b);
        return b;
    }
}
