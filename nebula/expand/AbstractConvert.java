package com.jwwd.flow.nebula.expand;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

public abstract class AbstractConvert {

    protected List<Field> fieldList = new LinkedList<>();

    protected String fieldStr() {
        StringJoiner fieldSj = new StringJoiner(",");
        for (Field field : fieldList) {
            fieldSj.add(field.getName());
        }
        return fieldSj.toString();
    }

    protected <T> void appendNGql(T t, StringBuilder sb) throws IllegalAccessException {
        int i = 0, size = fieldList.size();
        for (Field field : fieldList) {
            Object value = field.get(t);
            if (value == null) {
                sb.append("NULL");
            } else {
                if (field.getType() == String.class) {
                    String str = value.toString();
                    if (str.contains("\"")) {
                        str = str.replace("\"", "\\\"");
                    }
                    sb.append("\"").append(str).append("\"");
                } else if (field.getType() == boolean.class || field.getType() == Boolean.class) {
                    sb.append(value.toString().toLowerCase());
                } else {
                    sb.append(value);
                }
            }
            if (++i != size) sb.append(",");
        }
    }

    public void closeField() {
        for (Field field : fieldList) {
            field.setAccessible(false);
        }
        childCloseField();
    }

    protected abstract void childCloseField();
}