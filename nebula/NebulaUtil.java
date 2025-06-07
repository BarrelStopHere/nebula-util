package com.jwwd.flow.nebula;

import com.jwwd.common.core.utils.SpringUtils;
import com.jwwd.common.core.utils.StringUtils;
import com.jwwd.flow.nebula.annotation.NebulaIgnore;
import com.jwwd.flow.nebula.expand.*;
import com.jwwd.flow.nebula.space.Space;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 实现基本的nebula操作
 */
@Slf4j
public class NebulaUtil {
    private volatile static NebulaUtil instance;
    private final Map<Space, ExpandSessionManager> nebulaSessionsManagers;

    private NebulaUtil(Map<Space, ExpandSessionManager> nebulaSessionsManagers) {
        this.nebulaSessionsManagers = nebulaSessionsManagers;
    }

    public static NebulaUtil getInstance() {
        if (instance == null) {
            synchronized (NebulaUtil.class) {
                if (instance == null) {
                    instance = new NebulaUtil(SpringUtils.getBean("nebulaSessionsManagers"));
                }
            }
        }
        return instance;
    }

    public <T> List<T> list(String nGql, Class<T> clazz) {
        return list(nGql, Space.COMPANY_INFO, clazz);
    }

    public <T> List<T> list(String nGql, Space space, Class<T> clazz) {
        ResultSet resultSet = execute(nGql, space);
        if (resultSet == null) {
            return Collections.emptyList();
        }
        return resultSetToList(resultSet, clazz);
    }

    public <T> void insertVertex(List<T> entities, String tagName, Space space) {
        VertexConvert<T> converter = new VertexConvert<>(entities);
        String nGql = converter.build(tagName);
        execute(nGql, space);
    }

    public <T> void insertEdge(List<T> entities, String edgeName, Space space) {
        EdgeConvert<T> converter = new EdgeConvert<>(entities);
        String nGql = converter.build(edgeName);
        execute(nGql, space);
    }

    // 执行nGql语句
    public ResultSet execute(String nGql, Space space) {
        ExpandSessionManager sessionManager = nebulaSessionsManagers.get(space);
        ExpandSessionWrapper sessionWrapper = null;
        try {
            sessionWrapper = sessionManager.getSessionWrapper();
            Session session = sessionWrapper.getSession();
            log.info("execute nGql:{}", nGql);
            ResultSet resultSet = session.execute(nGql);
            if (resultSet != null) {
                if (StringUtils.isNotBlank(resultSet.getErrorMessage())) {
                    log.error("执行错误:{}", resultSet.getErrorMessage());
                }
                return resultSet;
            }
        } catch (Exception e) {
            throw new NebulaException(e.getMessage());
        } finally {
            if (sessionWrapper != null) {
                // 使用完后归还sessionWrapper的使用权
                sessionManager.returnSessionWrapper(sessionWrapper);
            }
        }
        return null;
    }

    private <T> List<T> resultSetToList(ResultSet rs, Class<T> clazz) throws NebulaException {
        List<T> result = new ArrayList<>();
        List<String> columns = rs.getColumnNames();
        List<Row> rows = rs.getRows();

        // 预处理字段映射关系（排除被 @NebulaIgnore 标记的字段）
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(NebulaIgnore.class)) {
                field.setAccessible(true);
                fieldMap.put(field.getName(), field); // 保留原始字段名
            }
        }

        for (Row row : rows) {
            T instance;
            try {
                Constructor<T> constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
                instance = constructor.newInstance();
            } catch (Exception e) {
                throw new NebulaException("无法实例化对象: " + clazz.getName());
            }

            for (int i = 0; i < columns.size(); i++) {
                String colName = columns.get(i);
                String[] split = colName.split("\\.");
                colName = split[split.length - 1];
                String camelName = toCamelCase(colName); // 转换为驼峰式
                Field field = fieldMap.get(camelName); // 根据驼峰名查找字段
                if (field != null) {
                    Value value = row.getValues().get(i);
                    Object valueObj = getValueByType(value);
                    try {
                        field.set(instance, valueObj);
                    } catch (IllegalAccessException e) {
                        throw new NebulaException("设置字段[" + field.getName() + "]值失败");
                    }
                }
            }
            result.add(instance);
        }
        return result;
    }

    public Object getValueByType(Value value) {
        int fieldType = value.getSetField();
        switch (fieldType) {
            case 1: // NullType
                return null;
            case 2: // Boolean
                return value.isBVal();
            case 3: // Long
                return value.getIVal();
            case 4: // Double
                return value.getFVal();
            case 5: // 字节数组转字符串
                return new String(value.getSVal(), StandardCharsets.UTF_8);
            case 6: // Date
                return value.getDVal().toString();
            case 7: // Time
                return value.getTVal().toString();
            case 8: // DateTime
                return value.getDtVal().toString();
            case 9: // Vertex
                return value.getVVal().toString();
            case 10: // Edge
                return value.getEVal().toString();
            case 11: // Path
                return value.getPVal().toString();
            case 12: // NList
                return value.getLVal().toString();
            case 13: // NMap
                return value.getMVal().toString();
            case 14: // NSet
                return value.getUVal().toString();
            case 15: // DataSet
                return value.getGVal().toString();
            case 16: // Geography
                return value.getGgVal().toString();
            case 17: // Duration
                return value.getDuVal().toString();
            default:
                return "未知类型";
        }
    }

    private static String toCamelCase(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        s = s.toLowerCase().replaceAll("_{2,}", "_"); // 处理连续下划线
        StringBuilder sb = new StringBuilder();
        boolean nextUpperCase = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                nextUpperCase = true;
            } else {
                if (nextUpperCase) {
                    sb.append(Character.toUpperCase(c));
                    nextUpperCase = false;
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }
}
