//package com.jwwd.flow.nebula.config;
//
//import com.jwwd.flow.nebula.space.Space;
//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
//import org.apache.ibatis.session.SqlSessionFactory;
//import org.mybatis.spring.SqlSessionFactoryBean;
//import org.mybatis.spring.annotation.MapperScan;
//import org.mybatis.spring.mapper.MapperScannerConfigurer;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
//import org.springframework.cloud.context.config.annotation.RefreshScope;
//import org.springframework.context.annotation.*;
//import org.springframework.core.env.Environment;
//import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.PostConstruct;
//import javax.sql.DataSource;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.ThreadLocalRandom;
//
//@Configuration
//@RefreshScope  // 全局启用配置刷新
//@MapperScan(basePackages = "com.jwwd.flow.nebula.nebula_mapper", sqlSessionFactoryRef = "nebulaSqlSessionFactory")
//@ConditionalOnProperty(name = "nebula.enabled", havingValue = "true")
//public class NebulaAutoConfiguration {
//
//    private static final Logger log = LoggerFactory.getLogger(NebulaAutoConfiguration.class);
//    private static final String JDBC_PREFIX = "jdbc:nebula://";
//
//    @Autowired
//    private Environment environment;
//
//    @PostConstruct
//    private void validateConfiguration() {
//        String url = environment.getProperty("nebula.url");
//        if (url == null || !url.startsWith(JDBC_PREFIX)) {
//            throw new IllegalArgumentException("Invalid nebula.url format");
//        }
//    }
//
//    @Bean
//    @RefreshScope  // 标记需要刷新的Bean
//    public DataSource nebulaDataSource() {
//        String url = environment.getProperty("nebula.url");
//        String username = environment.getProperty("nebula.username");
//        String password = environment.getProperty("nebula.password");
//        int poolSize = environment.getProperty("nebula.pool.size", Integer.class, 10);
//
//        if (url == null || username == null || password == null) {
//            throw new IllegalArgumentException("Missing required Nebula properties");
//        }
//
//        Map<Object, Object> dataSourceMap = getDataSourceMap(url, username, password, poolSize);
//        DynamicDataSource dynamicDataSource = new DynamicDataSource();
//        dynamicDataSource.setTargetDataSources(dataSourceMap);
//        Space defaultSpace = Space.getDefault();
//        dynamicDataSource.setDefaultTargetDataSource(dataSourceMap.get(defaultSpace.getKey()));
//        dynamicDataSource.afterPropertiesSet();
//        return dynamicDataSource;
//    }
//
//    @Bean
//    public SqlSessionFactory nebulaSqlSessionFactory(DataSource dataSource) throws Exception {
//        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
//        sessionFactory.setDataSource(dataSource);
//        sessionFactory.setMapperLocations(
//                new PathMatchingResourcePatternResolver()
//                        .getResources("classpath*:/nebula-mapper/*NebulaMapper.xml")
//        );
//        return sessionFactory.getObject();
//    }
//
//    @Bean
//    public MapperScannerConfigurer nebulaMapperScannerConfigurer() {
//        MapperScannerConfigurer configurer = new MapperScannerConfigurer();
//        configurer.setBasePackage("com.jwwd.flow.nebula.nebula_mapper");
//        configurer.setSqlSessionFactoryBeanName("nebulaSqlSessionFactory");
//        return configurer;
//    }
//
//    @PostConstruct
//    private void addShutdownHook() {
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                if (this.nebulaDataSource() instanceof HikariDataSource) {
//                    ((HikariDataSource) this.nebulaDataSource()).close();
//                }
//            } catch (Exception e) {
//                log.error("Error closing Nebula DataSource", e);
//            }
//        }));
//    }
//
//    private Map<Object, Object> getDataSourceMap(
//            String url,
//            String username,
//            String password,
//            int poolSize) {
//
//        Map<Object, Object> map = new HashMap<>();
//        for (Space space : Space.values()) {
//            HikariConfig config = buildHikariConfig(url, space, username, password, poolSize);
//            map.put(space.getKey(), new HikariDataSource(config));
//        }
//        return map;
//    }
//
//    private HikariConfig buildHikariConfig(
//            String url,
//            Space space,
//            String username,
//            String password,
//            int poolSize) {
//
//        HikariConfig config = new HikariConfig();
//        config.setDriverClassName("com.vesoft.nebula.jdbc.NebulaDriver");
//        config.setJdbcUrl(buildJdbcUrl(url, space));
//        config.setUsername(username);
//        config.setPassword(password);
//        config.setMaximumPoolSize(poolSize);
//        Properties props = new Properties();
//        props.setProperty("timeout", "60000");
//        config.setDataSourceProperties(props);
//        return config;
//    }
//
//    private String buildJdbcUrl(String base, Space space) {
//        String[] hosts = base.substring(JDBC_PREFIX.length()).split(",");
//        if (hosts.length == 0) {
//            throw new IllegalArgumentException("No valid Nebula hosts configured");
//        }
//        int pos = ThreadLocalRandom.current().nextInt(hosts.length);
//        return String.format("%s%s/%s", JDBC_PREFIX, hosts[pos], space.getKey());
//    }
//}