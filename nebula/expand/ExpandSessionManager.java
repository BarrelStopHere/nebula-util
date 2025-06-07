package com.jwwd.flow.nebula.expand;

import com.vesoft.nebula.client.graph.SessionsManagerConfig;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 扩展类
 * 提供release方法，供销毁问题session
 *
 * @see com.vesoft.nebula.client.graph.net.SessionsManager
 */
@Slf4j
public class ExpandSessionManager {

    private final SessionsManagerConfig config;
    private NebulaPool pool;
    private final CopyOnWriteArrayList<ExpandSessionWrapper> sessionList;
    private BitSet canUseBitSet;
    private final ScheduledExecutorService heartbeatExecutor;
    private boolean isClose = false;
    private volatile boolean isInitialized = false;
    private static final long HEARTBEAT_INTERVAL = 5 * 60;
    private static final int INITIAL_SESSIONS = 10;
    private final String USE_SPACE;

    public ExpandSessionManager(SessionsManagerConfig config) {
        this.config = config;
        this.sessionList = new CopyOnWriteArrayList<>();
        this.heartbeatExecutor = Executors.newScheduledThreadPool(1);
        this.USE_SPACE = "USE " + config.getSpaceName();
        checkConfig();
        try {
            init();
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to initialize ExpandSessionManager", e);
        }
    }

    private void checkConfig() {
        if (config.getAddresses().isEmpty()) {
            throw new RuntimeException("Empty graph addresses");
        }

        if (config.getSpaceName().isEmpty()) {
            throw new RuntimeException("Empty space name");
        }
    }

    private void init() throws RuntimeException {
        try {
            pool = new NebulaPool();
            if (!pool.init(config.getAddresses(), config.getPoolConfig())) {
                throw new RuntimeException("Init pool failed: services are broken.");
            }

            int maxConnSize = config.getPoolConfig().getMaxConnSize();
            canUseBitSet = new BitSet(maxConnSize);
            canUseBitSet.clear();

            // 启动心跳检测任务
            heartbeatExecutor.scheduleAtFixedRate(this::checkSessions, 0, HEARTBEAT_INTERVAL, TimeUnit.SECONDS);
            isInitialized = true;

            // 初始化时创建初始 session
            initializeSessions(maxConnSize);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Init the pool failed: " + e.getMessage());
        }
    }

    private void initializeSessions(int maxConnSize) {
        int targetCount = Math.min(INITIAL_SESSIONS, maxConnSize);
        log.info("Initializing {} sessions...", targetCount);

        for (int i = 0; i < targetCount; i++) {
            try {
                Session session = pool.getSession(config.getUserName(), config.getPassword(), config.getReconnect());
                ResultSet rs = session.execute(USE_SPACE);
                if (rs.isSucceeded()) {
                    ExpandSessionWrapper wrapper = new ExpandSessionWrapper(session);
                    sessionList.add(wrapper);
                    int index = sessionList.size() - 1;
                    canUseBitSet.set(index, true);
                } else {
                    log.error("初始化 session 失败: {}", rs.getErrorMessage());
                }
            } catch (Exception e) {
                log.error("创建初始 session 失败", e);
            }
        }
    }

    private void checkSessions() {
        if (!isInitialized || isClose) {
            return;
        }

        log.debug("开始心跳检测...");
        for (int i = 0; i < sessionList.size(); i++) {
            ExpandSessionWrapper session = sessionList.get(i);
            if (session == null) {
                continue;
            }

            boolean isValid = checkSessionValidity(session);
            if (!isValid) {
                replaceSession(i);
            }
        }
        log.debug("心跳检测完成.");
    }

    private boolean checkSessionValidity(ExpandSessionWrapper session) {
        try {
            ResultSet rs = session.getSession().execute(USE_SPACE);
            return rs.isSucceeded();
        } catch (Exception e) {
            log.warn("检测 session 失败: {}", e.getMessage());
            return false;
        }
    }

    private synchronized void replaceSession(int index) {
        if (index < 0 || index >= sessionList.size()) {
            log.warn("Invalid session index: {}", index);
            return;
        }

        ExpandSessionWrapper oldSession = sessionList.get(index);
        if (oldSession != null) {
            release(oldSession);
        }

        try {
            Session newSession = pool.getSession(config.getUserName(), config.getPassword(), config.getReconnect());
            ResultSet rs = newSession.execute(USE_SPACE);
            if (rs.isSucceeded()) {
                ExpandSessionWrapper newWrapper = new ExpandSessionWrapper(newSession);
                sessionList.set(index, newWrapper); // 直接替换
                canUseBitSet.set(index, true); // 标记为可用
                log.info("成功替换 session（索引：{}）", index);
            } else {
                log.error("创建新 session 失败：{}", rs.getErrorMessage());
            }
        } catch (Exception e) {
            log.error("替换 session 失败", e);
        }
    }

    /**
     * getSessionWrapper: return a SessionWrapper from sessionManager,
     * the SessionWrapper couldn't use by multi-thread
     *
     * @return SessionWrapper
     * @throws RuntimeException the exception when get SessionWrapper
     */
    public synchronized ExpandSessionWrapper getSessionWrapper() throws RuntimeException,
            ClientServerIncompatibleException {
        checkClose();
        if (pool == null) {
            init();
        }
        if (canUseBitSet.isEmpty()
                && sessionList.size() >= config.getPoolConfig().getMaxConnSize()) {
            throw new RuntimeException("The SessionsManager does not have available sessions.");
        }
        if (!canUseBitSet.isEmpty()) {
            int index = canUseBitSet.nextSetBit(0);
            if (index >= 0) {
                if (canUseBitSet.get(index)) {
                    canUseBitSet.set(index, false);
                    return sessionList.get(index);
                }
            }
        }
        try {
            Session session = pool.getSession(
                    config.getUserName(), config.getPassword(), config.getReconnect());
            ResultSet resultSet = session.execute(USE_SPACE);
            if (!resultSet.isSucceeded()) {
                throw new RuntimeException(
                        "Switch space `"
                                + config.getSpaceName()
                                + "' failed: "
                                + resultSet.getErrorMessage());
            }
            ExpandSessionWrapper sessionWrapper = new ExpandSessionWrapper(session);
            sessionList.add(sessionWrapper);
            return sessionWrapper;
        } catch (AuthFailedException | NotValidConnectionException | IOErrorException e) {
            throw new RuntimeException("Get session failed: " + e.getMessage());
        }
    }

    /**
     * returnSessionWrapper: return the SessionWrapper to the sessionManger,
     * the old SessionWrapper couldn't use again.
     *
     * @param session The SessionWrapper
     */
    public synchronized void returnSessionWrapper(ExpandSessionWrapper session) {
        checkClose();
        if (session == null) {
            return;
        }
        int index = sessionList.indexOf(session);
        if (index >= 0) {
            Session ses = session.getSession();
            sessionList.set(index, new ExpandSessionWrapper(ses));
            session.setNoAvailable();
            canUseBitSet.set(index, true);
        }
    }

    /**
     * close: release all sessions and close the connection pool
     */
    public synchronized void close(String userName) {
        // 释放当前角色的所有session
        try {
            heartbeatExecutor.shutdownNow();
            ExpandSessionWrapper sessionWrapper = getSessionWrapper();
            Session session = sessionWrapper.getSession();
            session.execute("SHOW SESSIONS | YIELD $-.SessionId as sid WHERE $-.UserName == \"" + userName + "\" | KILL SESSIONS $-.sid");
        } catch (Exception e) {
            log.error("nebula 释放session失败，请手动释放！");
        }
        if (pool != null) {
            pool.close();
        }
        sessionList.clear();
        isClose = true;
    }

    /**
     * release: release the session
     *
     * @apiNote 扩充方法，手动释放session
     */
    public synchronized void release(ExpandSessionWrapper sessionWrapper) {
        sessionWrapper.setNoAvailable();
        sessionWrapper.release();
        this.sessionList.remove(sessionWrapper);
    }

    private void checkClose() {
        if (isClose) {
            throw new RuntimeException("The SessionsManager was closed.");
        }
    }
}
