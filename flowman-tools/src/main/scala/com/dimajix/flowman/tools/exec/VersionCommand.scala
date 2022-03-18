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

package com.dimajix.flowman.tools.exec

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.HADOOP_BUILD_VERSION
import com.dimajix.flowman.HADOOP_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.SCALA_BUILD_VERSION
import com.dimajix.flowman.SCALA_VERSION
import com.dimajix.flowman.SPARK_BUILD_VERSION
import com.dimajix.flowman.SPARK_VERSION
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project


class VersionCommand extends Command {
    override def execute(session: Session, project: Project, context: Context): Status = {
        println(s"Flowman version $FLOWMAN_VERSION")
        println(s"Flowman home directory ${ToolConfig.homeDirectory.getOrElse("<unknown>")}")
        println(s"Flowman config directory ${ToolConfig.confDirectory.getOrElse("<unknown>")}")
        println(s"Spark version $SPARK_VERSION (build for $SPARK_BUILD_VERSION)")
        println(s"Hadoop version $HADOOP_VERSION (build for $HADOOP_BUILD_VERSION)")
        println(s"Scala version $SCALA_VERSION (build for $SCALA_BUILD_VERSION)")
        println(s"Java version $JAVA_VERSION")
        Status.SUCCESS
    }
}
