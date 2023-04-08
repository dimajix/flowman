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


package object flowman {
    val FLOWMAN_VERSION: String = Versions.FLOWMAN_VERSION
    val FLOWMAN_LOGO: String = Versions.FLOWMAN_LOGO
    val JAVA_VERSION: String = Versions.JAVA_VERSION
    val SCALA_VERSION: String = scala.util.Properties.versionNumberString
    val SPARK_BUILD_VERSION: String = Versions.SPARK_BUILD_VERSION
    val HADOOP_BUILD_VERSION: String = Versions.HADOOP_BUILD_VERSION
    val SCALA_BUILD_VERSION: String = Versions.SCALA_BUILD_VERSION
}
