/*
 * Copyright (C) 2023 The Flowman Authors
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

import java.io.StringWriter;

import lombok.val;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;


public class Velocity {
    private static final org.apache.velocity.app.VelocityEngine engine = new VelocityEngine();
    {
        engine.setProperty(RuntimeConstants.VM_ARGUMENTS_STRICT, "true");
        engine.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true");
        engine.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT_ESCAPE, "true");
        engine.init();
    }

    public static VelocityContext newContext() {
        val context = new VelocityContext();
        context.put("Boolean", new BooleanWrapper());
        context.put("Duration", new DurationWrapper());
        context.put("Float", new FloatWrapper());
        context.put("Integer", new IntegerWrapper());
        context.put("LocalDate", new LocalDateWrapper());
        context.put("LocalDateTime", new LocalDateTimeWrapper());
        context.put("Period", new PeriodWrapper());
        context.put("String", new StringWrapper());
        context.put("System", new SystemWrapper());
        context.put("URL", new URLWrapper());
        return context;
    }

    public static String evaluate(VelocityContext context, String logTag, String template) {
        StringWriter output = new StringWriter();
        engine.evaluate(context, output, logTag, template);
        return output.toString();
    }
}
