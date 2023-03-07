/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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


package object spark {
    final val SPARK_VERSION = org.apache.spark.SPARK_VERSION
    final val SPARK_VERSION_MAJOR = majorMinor(SPARK_VERSION)._1
    final val SPARK_VERSION_MINOR = majorMinor(SPARK_VERSION)._2

    /**
     * Given a Spark version string, return the (major version number, minor version number).
     * E.g., for 2.0.1-SNAPSHOT, return (2, 0).
     */
    private def majorMinor(sparkVersion: String): (Int, Int) = {
        val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r
        majorMinorRegex.findFirstMatchIn(sparkVersion) match {
            case Some(m) =>
                (m.group(1).toInt, m.group(2).toInt)
            case None =>
                throw new IllegalArgumentException(s"Spark tried to parse '$sparkVersion' as a Spark" +
                    " version string, but it could not find the major and minor version numbers.")
        }
    }
}
