/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools

import java.io.File


object ToolConfig {
    def homeDirectory : Option[File] = Option(System.getenv("FLOWMAN_HOME"))
        .filter(_.nonEmpty)
        .map(new File(_))
        .filter(_.isDirectory)

    def confDirectory : Option[File] = Option(System.getenv("FLOWMAN_CONF_DIR"))
        .filter(_.nonEmpty)
        .map(new File(_))
        .orElse(homeDirectory.map(new File(_ , "conf")))
        .filter(_.isDirectory)

    def pluginDirectory : Option[File] = Option(System.getenv("FLOWMAN_PLUGIN_DIR"))
        .filter(_.nonEmpty)
        .map(new File(_))
        .orElse(homeDirectory.map(new File(_, "plugins")))
        .filter(_.isDirectory)
}
