package com.jwwd.flow.nebula.expand;

import com.jwwd.flow.nebula.annotation.NebulaIgnore;
import com.jwwd.flow.nebula.annotation.Vid;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.List;
import java.util.StringJoiner;

@Slf4j
public class VertexConvert<T> extends AbstractConvert {

    private Field vidField;
    private final List<T> entityList;

    public VertexConvert(List<T> entityList) {
        this.entityList = entityList;
    }

    public String build(String tagName) {
        if (entityList.isEmpty()) return "";
        buildField(entityList.get(0));
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT VERTEX ").append(tagName).append("(").append(fieldStr()).append(") VALUES ");
        StringJoiner valuesSj = new StringJoiner(",");
        for (T entity : entityList) {
            String value = buildValue(entity);
            if (value != null) valuesSj.add(value);
        }
        sb.append(valuesSj);
        closeField();
        return sb.toString();
    }

    private String buildValue(T entity) {
        StringBuilder sb = new StringBuilder();
        try {
            Object id = vidField.get(entity);
            if (id == null) {
                throw new IllegalArgumentException("顶点ID字段不能为空");
            }
            sb.append(id).append(":(");
            appendNGql(entity, sb);
            sb.append(")");
            return sb.toString();
        } catch (IllegalAccessException e) {
            log.error("构建顶点值失败: {}", e.getMessage());
            return null;
        }
    }

    private void buildField(T entity) {
        Field[] fields = entity.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(Vid.class)) {
                vidField = field;
                continue;
            }
            if (!field.isAnnotationPresent(NebulaIgnore.class)) {
                fieldList.add(field);
            }
        }
        if (vidField == null) {
            throw new IllegalArgumentException("顶点必须包含@Id注解的ID字段");
        }
    }

    @Override
    protected void childCloseField() {
        vidField.setAccessible(false);
    }
}