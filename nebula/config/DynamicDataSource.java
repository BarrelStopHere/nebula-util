package com.jwwd.flow.nebula.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.Map;

public class DynamicDataSource extends AbstractRoutingDataSource implements DisposableBean {
    @Override
    protected Object determineCurrentLookupKey() {
        return SpaceHolder.getSpace();
    }

    // 新的 close 方法，遍历所有底层数据源并关闭
    public void close() {
        Map<Object, DataSource> targetDataSources = getResolvedDataSources();
        for (DataSource ds : targetDataSources.values()) {
            if (ds instanceof HikariDataSource) {
                ((HikariDataSource) ds).close();
            }
        }
    }

    // 实现 DisposableBean 接口，让 Spring 自动调用关闭
    @Override
    public void destroy() throws Exception {
        close();
    }
}
