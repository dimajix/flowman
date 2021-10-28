/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.dimajix.flowman.plugin.Plugin


object PluginListener {
    def listeners : Seq[PluginListener] = {
        val loader = ServiceLoader.load(classOf[PluginListener])
        loader.iterator().asScala.toSeq
    }
}

/**
 * The PluginListener gets called whenever a new plugin has been loaded. This allows Flowman or plugins to clean
 * up some classpath caches or do similar work.
 */
trait PluginListener {
    /**
     * Called when a new Plugin has been loaded and its code is available.
     * @param plugin
     * @param classLoader
     */
    def pluginLoaded(plugin:Plugin, classLoader: ClassLoader) : Unit
}
