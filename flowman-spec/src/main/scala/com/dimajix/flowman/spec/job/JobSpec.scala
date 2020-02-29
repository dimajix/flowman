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

package com.dimajix.flowman.spec.job

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.metric.MetricBoardSpec
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StringType

object JobSpec extends TypeRegistry[JobSpec] {
    class NameResolver extends StdConverter[Map[String, JobSpec], Map[String, JobSpec]] {
        override def convert(value: Map[String, JobSpec]): Map[String, JobSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }

    class Parameter extends Spec[Job.Parameter] {
        @JsonProperty(value = "name") private var name: String = ""
        @JsonProperty(value = "description") private var description: Option[String] = None
        @JsonProperty(value = "type", required = false) private var ftype: FieldType = StringType
        @JsonProperty(value = "granularity", required = false) private var granularity: Option[String] = None
        @JsonProperty(value = "default", required = false) private var default: Option[String] = None

        override def instantiate(context: Context): Job.Parameter = {
            require(context != null)

            Job.Parameter(
                context.evaluate(name),
                ftype,
                granularity.map(context.evaluate),
                default.map(context.evaluate).map(d => ftype.parse(d)),
                description.map(context.evaluate)
            )
        }
    }
}

class JobSpec extends NamedSpec[Job] {
    @JsonProperty(value="extends") private var parents:Seq[String] = Seq()
    @JsonProperty(value="description") private var description:Option[String] = None
    @JsonProperty(value="parameters") private var parameters:Seq[JobSpec.Parameter] = Seq()
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="targets") private var targets: Seq[String] = Seq()
    @JsonProperty(value="metrics") private var metrics:Option[MetricBoardSpec] = None

    override def instantiate(context: Context): Job = {
        require(context != null)

        val parents = this.parents.map(job => context.getJob(JobIdentifier(job)))

        val parentParameters = parents
            .map(job => job.parameters.map(p => (p.name, p)).toMap)
            .reduceOption((params, elems) => params ++ elems)
            .getOrElse(Map())
        val parentEnvironment = parents
            .map(job => job.environment)
            .reduceOption((envs, elems) => envs ++ elems)
            .getOrElse(Map())
        val parentTargets = parents
            .map(job => job.targets.toSet)
            .reduceOption((targets, elems) => targets ++ elems)
            .getOrElse(Set())
        val parentMetrics = parents
            .flatMap(job => job.metrics)
            .headOption

        val curEnvironment = splitSettings(environment).toMap
        val allEnvironment = parentEnvironment ++ curEnvironment

        val curParameters = parameters.map(_.instantiate(context)).map(p => (p.name,p)).toMap
        val allParameters = parentParameters -- allEnvironment.keySet ++ curParameters

        val curTargets = targets.map(context.evaluate).map(TargetIdentifier.parse)
        val allTargets = parentTargets ++ curTargets

        val allMetrics = metrics.orElse(parentMetrics)

        Job(
            instanceProperties(context),
            description.map(context.evaluate),
            allParameters.values.toSeq,
            allEnvironment,
            allTargets.toSeq,
            allMetrics
        )
    }

    /**
      * Returns a set of common properties
      *
      * @param context
      * @return
      */
    override protected def instanceProperties(context: Context): Job.Properties = {
        require(context != null)
        Job.Properties(
            context,
            context.namespace,
            context.project,
            name,
            context.evaluate(labels)
        )
    }
}
