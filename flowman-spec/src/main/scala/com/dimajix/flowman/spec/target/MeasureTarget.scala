/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.spec.target

import java.time.Clock
import java.time.Instant

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.metric.LongAccumulatorMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.metric.SettableGaugeMetric
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.measure.MeasureSpec
import com.dimajix.spark.sql.DataFrameUtils


case class MeasureTarget(
    instanceProperties: Target.Properties,
    measures: Map[String,Measure]
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[MeasureTarget])

    /**
     * Returns an instance representing this target with the context
     *
     * @return
     */
    override def digest(phase:Phase): TargetDigest = {
        // Create a custom instance identifier with a timestamp, such that every run is a new instance. Otherwise
        // validation wouldn't be always executed in the presence of a state store.
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            Map("measure_ts" -> Clock.systemUTC().millis().toString)
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VERIFY)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.VERIFY => measures.flatMap(_._2.requires).toSet
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
     * Performs a verification of the build step or possibly other checks.
     *
     * @param executor
     */
    protected override def verify2(execution: Execution): TargetResult = {
        val startTime = Instant.now()

        // Collect all required DataFrames for caching. We assume that each DataFrame might be used in multiple
        // measures and that the DataFrames aren't very huge (we are talking about tests!)
        val measures = this.measures.values.toList
        val inputDataFrames = measures
            .flatMap(instance => instance.inputs)
            .map(id => execution.instantiate(context.getMapping(id.mapping), id.output))

        val cacheLevel = StorageLevel.NONE
        val metadata = this.metadata.asMap + ("phase" -> Phase.VERIFY.upper)
        val result = DataFrameUtils.withCaches(inputDataFrames, cacheLevel) {
            measures.map { measure =>
                execution.monitorMeasure(measure) { execution =>
                    val result = execution.measure(measure)
                    val measurements = result.measurements.map { m =>
                        m.copy(labels = metadata ++ m.labels)
                    }
                    result.copy(measurements=measurements)
                }
            }
        }

        // Publish result as metrics
        val metrics = execution.metricSystem
        result.flatMap(_.measurements).foreach { measurement =>
            val gauge = metrics.findMetric(Selector(measurement.name, measurement.labels))
                .headOption
                .map(_.asInstanceOf[SettableGaugeMetric])
                .getOrElse {
                    val metric = SettableGaugeMetric(measurement.name, measurement.labels)
                    metrics.addMetric(metric)
                    metric
                }
            gauge.value = measurement.value
            measurement
        }

        TargetResult(this, Phase.VERIFY, result, startTime)
    }
}



class MeasureTargetSpec extends TargetSpec {
    @JsonDeserialize(converter=classOf[MeasureSpec.NameResolver])
    @JsonProperty(value = "measures", required = true) private var measures: Map[String,MeasureSpec] = Map()

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): MeasureTarget = {
        MeasureTarget(
            instanceProperties(context, properties),
            measures.map { case(name,measure) => name -> measure.instantiate(context) }
        )
    }
}
