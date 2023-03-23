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

package com.dimajix.common

import java.net.URL
import java.util.Properties


class Resources
object Resources {
    def getURL(resourceName:String) : URL = {
        val loader = Thread.currentThread.getContextClassLoader
        loader.getResource(resourceName)
    }

    def loadProperties(resourceName:String) : Properties = {
        val loader = Thread.currentThread.getContextClassLoader
        val url = loader.getResource(resourceName)
        loadProperties(url)
    }

    def loadProperties(contextClass:Class[_], resourceName:String) : Properties = {
        val url = com.google.common.io.Resources.getResource(contextClass, resourceName)
        loadProperties(url)
    }

    private def loadProperties(url:URL) : Properties = {
        val inputStream = url.openStream()
        try {
            val props = new Properties()
            props.load(inputStream)
            props
        } finally {
            inputStream.close()
        }
    }
}
