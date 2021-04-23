/*
 * Copyright 2021 Kaya Kupferschmidt
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
    final val SPARK_VERSION = org.apache.spark.SPARK_VERSION
    final val JAVA_VERSION = System.getProperty("java.version")
    final val FLOWMAN_VERSION = {
        Resources.loadProperties("com/dimajix/flowman/flowman.properties")
            .getProperty("version")
    }
}
