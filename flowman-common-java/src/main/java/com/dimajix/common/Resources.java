/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.common;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import lombok.val;


public final class Resources {
    private Resources() {
    }

    public static URL getURL(String resourceName) {
        val loader = Thread.currentThread().getContextClassLoader();
        return loader.getResource(resourceName);
    }

    public static Properties loadProperties(String resourceName) throws IOException {
        val loader = Thread.currentThread().getContextClassLoader();
        val url = loader.getResource(resourceName);
        return loadProperties(url);
    }

    public static Properties loadProperties(Class<?> contextClass, String resourceName) throws IOException {
        val url = com.google.common.io.Resources.getResource(contextClass, resourceName);
        return loadProperties(url);
    }

    private static Properties loadProperties(URL url) throws IOException {
        val inputStream = url.openStream();
        try {
            val props = new Properties();
            props.load(inputStream);
            return props;
        } finally {
            inputStream.close();
        }
    }
}
