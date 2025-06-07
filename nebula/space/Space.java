package com.jwwd.flow.nebula.space;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Space {

    COMPANY_INFO("COMPANY_INFO","公司项目工地图空间"),;

    private final String key;
    private final String name;

    public static Space of(String key){
        for (Space value : values()) {
            if (value.key.equals(key)){
                return value;
            }
        }
        return COMPANY_INFO;
    }

    public static Space getDefault(){
        return COMPANY_INFO;
    }
}
