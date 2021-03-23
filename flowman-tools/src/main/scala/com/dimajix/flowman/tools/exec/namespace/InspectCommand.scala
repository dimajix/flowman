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

package com.dimajix.flowman.tools.exec.namespace

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class InspectCommand extends Command {
    override def execute(session: Session, project:Project, context:Context): Boolean = {
        session.namespace.foreach { ns =>
            println("Namespace:")
            println(s"    name: ${ns.name}")
            println(s"    plugins: ${ns.plugins.mkString(",")}")

            println("Environment:")
            ns.environment
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }

            println("Configuration:")
            ns.config
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }

            println("Profiles:")
            ns.profiles
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }

            println("Connections:")
            ns.connections
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }
        }

        true
    }
}
