package com.jwwd.flow.nebula.config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpaceHolder {
    private static final ThreadLocal<String> space = new ThreadLocal<>();

    public static void setSpace(String space) {
        SpaceHolder.space.set(space);
    }

    public static String getSpace() {
        return space.get();
    }

    public static void clearSpace() {
        space.remove();
    }
}
