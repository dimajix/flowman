/*
 * Copyright 2020 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.templating;

import com.dimajix.flowman.annotation.TemplateObject;
import com.dimajix.flowman.spi.ClassAnnotationHandler;
import com.dimajix.flowman.spi.ClassAnnotationScanner;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class Velocity {
    private static Logger log = LoggerFactory.getLogger(Velocity.class);

    private static Map<String,Class> classes = new HashMap<>();
    private static Map<String,Object> objects = new HashMap<>();

    static public void addClass(String name, Class<?> aClass) {
        classes.put(name, aClass);
    }
    static public void addObject(String name, Object obj) {
        objects.put(name, obj);
    }

    static {
        addObject("Boolean", BooleanWrapper$.MODULE$);
        addObject("Integer", IntegerWrapper$.MODULE$);
        addObject("Float", FloatWrapper$.MODULE$);
        addObject("LocalDate", LocalDateWrapper$.MODULE$);
        addObject("LocalDateTime", LocalDateTimeWrapper$.MODULE$);
        addObject("Timestamp", TimestampWrapper$.MODULE$);
        addObject("Duration", DurationWrapper$.MODULE$);
        addObject("Period", PeriodWrapper$.MODULE$);
        addObject("System", SystemWrapper$.MODULE$);
        addObject("String", StringWrapper$.MODULE$);
        addObject("URL", URLWrapper$.MODULE$);
    }

    /**
     * Creates a new VelocityContext with all templating objects preregistered in the context
     * @return
     */
    static public VelocityContext newContext() {
        // Ensure that all extensions are loaded
        ClassAnnotationScanner.load();

        VelocityContext context = new VelocityContext();

        // Add instances of all custom classes
        for (Map.Entry<String,Class> e : classes.entrySet()) {
            String name = e.getKey();
            Class<?> clazz = e.getValue();
            try {
                context.put(name, clazz.newInstance());
            } catch (InstantiationException|IllegalAccessException ex) {
                log.warn("Cannot add class instance '{}' of class {} to Velocity context", name, clazz.getCanonicalName(), ex);
            }
        }

        // Add all objects to context
        for (Map.Entry<String,Object> e : objects.entrySet()) {
            context.put(e.getKey(), e.getValue());
        }

        return context;
    }

    /**
     * Creates a new VelocityContext with the given context set as parent
     * @return
     */
    static public VelocityContext newContext(VelocityContext parent) {
        return new VelocityContext(parent);
    }

    /**
     * Creates a new VelocityEngine
     * @return
     */
    static public VelocityEngine newEngine() {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.VM_ARGUMENTS_STRICT, "true");
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true");
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT_ESCAPE, "true");
        ve.init();
        return ve;
    }
}
