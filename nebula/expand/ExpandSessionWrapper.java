package com.jwwd.flow.nebula.expand;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.InvalidSessionException;
import com.vesoft.nebula.client.graph.net.Session;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 扩展类，将私有方法变为共有，便于ExpandSessionManager调用
 * @see com.vesoft.nebula.client.graph.net.SessionWrapper
 * @see com.jwwd.flow.nebula.expand.ExpandSessionManager
 */
public class ExpandSessionWrapper {

    @Getter
    private final Session session;
    private final AtomicBoolean available = new AtomicBoolean(true);

    public ExpandSessionWrapper(Session session) {
        this.session = session;
    }

    /**
     * Execute the query sentence.
     *
     * @param stmt The query sentence.
     * @return The ResultSet.
     */
    public ResultSet execute(String stmt)
            throws IOErrorException {
        if (!available()) {
            throw new InvalidSessionException();
        }
        return session.execute(stmt);
    }

    /**
     * @apiNote default -> public
     */
    public void setNoAvailable() {
        this.available.set(false);
    }

    /**
     * @apiNote default -> public
     */
    public boolean available() {
        return available.get();
    }

    /**
     * @apiNote default -> public
     */
    public void release() {
        if (session != null) {
            session.release();
        }
    }

}
