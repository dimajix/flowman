/*
 * Copyright 2023 Kaya Kupferschmidt
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

import org.apache.velocity.runtime.RuntimeConstants;

public class VelocityEngine {
    private final org.apache.velocity.app.VelocityEngine engine;

    public VelocityEngine() {
        engine = new org.apache.velocity.app.VelocityEngine();
        engine.setProperty(RuntimeConstants.VM_ARGUMENTS_STRICT, "true");
        engine.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true");
        engine.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT_ESCAPE, "true");
        engine.init();
    }

    public String evaluate(VelocityContext context, String logTag, String template) {
        StringWriter output = new StringWriter();
        engine.evaluate(context.context, output, logTag, template);
        return output.toString();
    }
}
