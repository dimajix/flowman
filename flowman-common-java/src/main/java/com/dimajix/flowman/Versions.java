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

import lombok.val;

import com.dimajix.common.Resources;


public final class Versions {
    public final static String FLOWMAN_VERSION;
    public static final String FLOWMAN_LOGO;
    public static final String SPARK_BUILD_VERSION;
    public static final String HADOOP_BUILD_VERSION;
    public final static String JAVA_VERSION = System.getProperty("java.version");
    public static final String SCALA_BUILD_VERSION;

    static {
        try {
            val props = Resources.loadProperties("com/dimajix/flowman/flowman.properties");
            FLOWMAN_VERSION = props.getProperty("version");
            SPARK_BUILD_VERSION = props.getProperty("spark_version");
            HADOOP_BUILD_VERSION = props.getProperty("hadoop_version");
            SCALA_BUILD_VERSION = props.getProperty("scala_version");

            val logo = com.dimajix.common.Resources.loadResource("com/dimajix/flowman/flowman-logo.txt");
            FLOWMAN_LOGO = logo.substring(0, logo.length() - 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Versions() {
    }
}
