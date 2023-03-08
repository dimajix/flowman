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

package com.dimajix

import com.dimajix.common.Resources


package object flowman {
    private val props = Resources.loadProperties("com/dimajix/flowman/flowman.properties")
    val JAVA_VERSION: String = System.getProperty("java.version")
    val SCALA_VERSION: String = scala.util.Properties.versionNumberString
    val FLOWMAN_VERSION: String = props.getProperty("version")
    val SPARK_BUILD_VERSION: String = props.getProperty("spark_version")
    val HADOOP_BUILD_VERSION: String = props.getProperty("hadoop_version")
    val SCALA_BUILD_VERSION: String = props.getProperty("scala_version")
}
