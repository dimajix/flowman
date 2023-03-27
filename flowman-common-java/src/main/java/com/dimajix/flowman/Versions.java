/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman;


import java.io.IOException;
import java.util.Properties;

import com.dimajix.common.Resources;


public final class Versions {
    private final static Properties props;
    public final static String JAVA_VERSION = System.getProperty("java.version");
    public final static String FLOWMAN_VERSION;

    static {
        try {
            props = Resources.loadProperties("com/dimajix/flowman/flowman.properties");
            FLOWMAN_VERSION = props.getProperty("version");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Versions() {
    }
}
