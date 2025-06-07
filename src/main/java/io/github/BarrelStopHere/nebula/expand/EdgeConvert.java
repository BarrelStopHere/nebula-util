package io.github.BarrelStopHere.nebula.expand;

import io.github.BarrelStopHere.nebula.annotation.EdgeFrom;
import io.github.BarrelStopHere.nebula.annotation.EdgeRank;
import io.github.BarrelStopHere.nebula.annotation.EdgeTo;
import io.github.BarrelStopHere.nebula.annotation.NebulaIgnore;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.List;
import java.util.StringJoiner;

@Slf4j
public class EdgeConvert<T> extends AbstractConvert {

    private Field fromField;
    private Field toField;
    private Field rankField;
    private final List<T> entityList;

    public EdgeConvert(List<T> entityList) {
        this.entityList = entityList;
    }

    public String build(String edgeName) {
        if (entityList.isEmpty()) return "";
        buildField(entityList.get(0));
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT EDGE ").append(edgeName).append("(").append(fieldStr()).append(") VALUES ");
        StringJoiner valuesSj = new StringJoiner(",");
        for (T entity : entityList) {
            valuesSj.add(buildParam(entity));
        }
        sb.append(valuesSj);
        closeField();
        return sb.toString();
    }

    private String buildParam(T entity) {
        StringBuilder sb = new StringBuilder();
        try {
            Object from = fromField.get(entity);
            Object to = toField.get(entity);
            if (from == null || to == null) {
                throw new IllegalArgumentException("边起点或终点字段为空");
            }
            sb.append(from).append("->").append(to);
            if (rankField != null) {
                Object rank = rankField.get(entity);
                sb.append("@").append(rank);
            }
            sb.append(":(");
            appendNGql(entity, sb);
            sb.append(")");
            return sb.toString();
        } catch (IllegalAccessException e) {
            log.error("构建边参数失败: {}", e.getMessage());
            return null;
        }
    }

    private void buildField(T entity) {
        Field[] fields = entity.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(EdgeFrom.class)) {
                fromField = field;
                continue;
            }
            if (field.isAnnotationPresent(EdgeTo.class)) {
                toField = field;
                continue;
            }
            if (field.isAnnotationPresent(EdgeRank.class)) {
                rankField = field;
                continue;
            }
            if (!field.isAnnotationPresent(NebulaIgnore.class)) {
                fieldList.add(field);
            }
        }
        if (fromField == null || toField == null) {
            throw new IllegalArgumentException("边必须包含起点（@EdgeFrom）和终点（@EdgeTo）字段");
        }
    }

    @Override
    protected void childCloseField() {
        fromField.setAccessible(false);
        toField.setAccessible(false);
        if (rankField != null) rankField.setAccessible(false);
    }
}