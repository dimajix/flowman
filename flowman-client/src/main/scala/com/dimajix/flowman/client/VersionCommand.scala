/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.client

import java.net.URI

import org.apache.http.impl.client.CloseableHttpClient

import com.dimajix.flowman.common.ToolConfig


class VersionCommand extends Command {
    override def execute(httpClient:CloseableHttpClient, baseUri:URI): Boolean = {
        println(s"Flowman version $FLOWMAN_VERSION")
        println(s"Flowman home directory ${ToolConfig.homeDirectory.getOrElse("<unknown>")}")
        println(s"Flowman config directory ${ToolConfig.confDirectory.getOrElse("<unknown>")}")
        println(s"Spark build version $SPARK_BUILD_VERSION")
        println(s"Hadoop build version $HADOOP_BUILD_VERSION")
        println(s"Scala version $SCALA_VERSION (build for $SCALA_BUILD_VERSION)")
        println(s"Java version $JAVA_VERSION")
        true
    }
}
