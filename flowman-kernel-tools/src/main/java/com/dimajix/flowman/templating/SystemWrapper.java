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

import lombok.val;


public class SystemWrapper {
    public String getenv(String name) {
        val r = System.getenv(name);
        if (r != null)
            return r;
        else
            return "";
    }
    public String getenv(String name, String defaultValue) {
        val r = System.getenv(name);
        if (r != null)
            return r;
        else
            return defaultValue;
    }

    public String getProperty(String name) {
        val r = System.getProperty(name);
        if (r != null)
            return r;
        else
            return "";
    }
    public String getProperty(String name, String defaultValue) {
        val r = System.getProperty(name);
        if (r != null)
            return r;
        else
            return defaultValue;
    }
}
