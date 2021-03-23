/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.project

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class InspectCommand extends Command {
    override def execute(session: Session, project:Project, context:Context): Boolean = {
        println("Project:")
        println(s"    name: ${project.name}")
        println(s"    version: ${project.version.getOrElse("")}")
        println(s"    description: ${project.description.getOrElse("")}")
        println(s"    basedir: ${project.basedir.getOrElse("")}")
        println(s"    filename: ${project.filename.map(_.toString).getOrElse("")}")
        println("Environment:")
        project.environment
            .toSeq
            .sortBy(_._1)
            .foreach{ case(k,v) => println(s"    $k=$v") }
        println("Configuration:")
        project.config
            .toSeq
            .sortBy(_._1)
            .foreach{ case(k,v) => println(s"    $k=$v") }
        println("Profiles:")
        project.profiles
            .toSeq
            .sortBy(_._1)
            .foreach{ p => println(s"    ${p._1}") }
        println("Mappings:")
        project.mappings
            .toSeq
            .sortBy(_._1)
            .foreach{ p => println(s"    ${p._1}") }
        println("Relations:")
        project.relations
            .toSeq
            .sortBy(_._1)
            .foreach{ p => println(s"    ${p._1}") }
        println("Jobs:")
        project.jobs
            .toSeq
            .sortBy(_._1)
            .foreach{ p => println(s"    ${p._1}") }
        println("Targets:")
        project.targets
            .toSeq
            .sortBy(_._1)
            .foreach{ p => println(s"    ${p._1}") }
        true
    }
}
