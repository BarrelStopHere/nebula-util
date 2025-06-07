package com.jwwd.flow.nebula.config;

import com.jwwd.flow.nebula.annotation.TargetSpace;
import com.jwwd.flow.nebula.space.Space;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * 使用@TargetSpace标注mapper指定操作的图空间
 */
@Aspect
@Component
public class SpaceExchange {

    /**
     * 基于注解方式的space切换
     */
    @Before("@annotation(com.jwwd.flow.nebula.annotation.TargetSpace)")
    public void annotationBefore(JoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        boolean present = method.isAnnotationPresent(TargetSpace.class);
        if (present) {
            TargetSpace target = method.getAnnotation(TargetSpace.class);
            SpaceHolder.setSpace(target.value());
        } else {
            if (joinPoint.getTarget().getClass().isAnnotationPresent(TargetSpace.class)) {
                TargetSpace target = joinPoint.getTarget().getClass().getAnnotation(TargetSpace.class);
                SpaceHolder.setSpace(target.value());
            }
        }
    }

    @After("@annotation(com.jwwd.flow.nebula.annotation.TargetSpace)")
    public void annotationAfter() {
        SpaceHolder.clearSpace();
    }

    /**
     * 基于参数的space切换
     * 如果参数是Space 按传入的值切换
     */
    @Before("execution(* com.jwwd.flow.nebula.nebula_mapper..*(..))")
    public void before(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        for (Object arg : args) {
             if (arg instanceof Space) {
                SpaceHolder.setSpace(((Space) arg).getKey());
                return;
            }
        }
    }

    @After("execution(* com.jwwd.flow.nebula.nebula_mapper..*(..))")
    public void after() {
        SpaceHolder.clearSpace();
    }
}
