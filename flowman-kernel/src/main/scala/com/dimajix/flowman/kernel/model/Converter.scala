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

package com.dimajix.flowman.kernel.model

import com.dimajix.flowman.kernel.service
import com.dimajix.flowman.model


object Converter {
    def of(ns:model.Namespace) : Namespace = {
        Namespace(
            ns.name,
            ns.environment,
            ns.config,
            ns.profiles.keys.toSeq,
            ns.connections.keys.toSeq,
            ns.plugins
        )
    }

    def of(project:model.Project) : Project = {
        Project(
            project.name,
            project.version,
            project.description,
            project.filename.map(_.toString),
            project.basedir.map(_.toString),
            project.environment,
            project.config,
            project.profiles.keys.toSeq,
            project.connections.keys.toSeq,
            project.jobs.keys.toSeq,
            project.targets.keys.toSeq
        )
    }

    def of(job:model.Job) : Job = {
        Job(
            job.name,
            job.description,
            job.parameters.map(_.name),
            job.environment
        )
    }

    def of(job:service.JobTask) : JobTask = {
        JobTask(
            job.id,
            job.job.identifier.toString,
            job.phase.toString,
            job.lifecycle.map(_.toString),
            job.rawArgs,
            job.force,
            job.keepGoing,
            job.dryRun,
            job.status.toString
        )
    }
}
