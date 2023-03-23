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


import java.util.Map;

public class VelocityContext {
    final org.apache.velocity.VelocityContext context;

    public VelocityContext() {
        context = new org.apache.velocity.VelocityContext();
    }
    public VelocityContext(VelocityContext parent) {
        context = new org.apache.velocity.VelocityContext(parent.context);
    }
    public VelocityContext(Map<String,Object> values, VelocityContext parent) {
        context = new org.apache.velocity.VelocityContext(values, parent.context);
    }

    public void put(String name, Object value) {
        context.put(name, value);
    }
}
