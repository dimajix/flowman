/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.model

import com.dimajix.flowman.history
import com.dimajix.flowman.model


object Converter {
    def ofSpec(ns:model.Namespace) : Namespace = {
        Namespace(
            ns.name,
            ns.environment,
            ns.config,
            ns.profiles.keys.toSeq,
            ns.connections.keys.toSeq,
            ns.plugins
        )
    }

    def ofSpec(project:model.Project) : Project = {
        Project(
            project.name,
            project.version,
            project.description,
            project.environment,
            project.config,
            project.profiles.keys.toSeq,
            project.connections.keys.toSeq,
            project.basedir.map(_.toString),
            project.jobs.keys.toSeq,
            project.targets.keys.toSeq
        )
    }

    def ofSpec(job:model.Job) : Job = {
        Job(
            job.name,
            job.description,
            job.parameters.map(_.name),
            job.environment
        )
    }

    def ofSpec(jobState:history.JobState) : JobState = {
        JobState(
            jobState.id,
            jobState.namespace,
            jobState.project,
            jobState.job,
            jobState.phase.toString,
            jobState.args,
            jobState.status.toString,
            jobState.startDateTime,
            jobState.endDateTime
        )
    }

    def ofSpec(state:history.TargetState) : TargetState = {
        TargetState()
    }
}
