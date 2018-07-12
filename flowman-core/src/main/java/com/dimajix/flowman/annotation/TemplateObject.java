package com.dimajix.flowman.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface TemplateObject {
    /**
     * Specifies the name under which the object will be available in template scripts
     * @return
     */
    String name();
}
