/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import java.util.Locale

import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.documentation.TargetDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.metric.LongAccumulatorMetric
import com.dimajix.flowman.metric.MetricBundle
import com.dimajix.flowman.metric.MultiMetricBundle
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.metric.WallTimeMetric
import com.dimajix.flowman.model
import com.dimajix.spark.sql.functions.count_records


sealed abstract class VerifyPolicy extends Product with Serializable
object VerifyPolicy {
    case object EMPTY_AS_SUCCESS extends VerifyPolicy
    case object EMPTY_AS_FAILURE extends VerifyPolicy
    case object EMPTY_AS_SUCCESS_WITH_ERRORS extends VerifyPolicy

    def ofString(mode:String) : VerifyPolicy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "empty_as_success" => VerifyPolicy.EMPTY_AS_SUCCESS
            case "empty_as_failure" => VerifyPolicy.EMPTY_AS_FAILURE
            case "empty_as_success_with_errors" => VerifyPolicy.EMPTY_AS_SUCCESS_WITH_ERRORS
            case _ => throw new IllegalArgumentException(s"Unknown verify policy: '$mode'. " +
                "Accepted verify policies are 'empty_as_success', 'empty_as_failure' and 'empty_as_success_with_errors'.")
        }
    }
}


/**
  *
  * @param namespace
  * @param project
  * @param target
  * @param partitions
  */
final case class TargetDigest(
    namespace:String,
    project:String,
    target:String,
    phase:Phase,
    partitions:Map[String,String] = Map.empty
) {
    def asMap: Map[String, String] =
        Map(
            "namespace" -> namespace,
            "project" -> project,
            "name" -> target,
            "target" -> target,
            "phase" -> phase.toString
        ) ++ partitions
}


object Target {
    object Properties {
        def apply(context: Context, name:String = "", kind:String="") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.TARGET, kind),
                Seq.empty,
                Seq.empty,
                None,
                None
            )
        }
    }
    final case class Properties(
        context:Context,
        metadata:Metadata,
        before:Seq[TargetIdentifier],
        after:Seq[TargetIdentifier],
        description:Option[String],
        documentation:Option[TargetDoc]
    ) extends model.Properties[Properties] {
        require(metadata.category == Category.TARGET.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(
                context,
                metadata.merge(other.metadata),
                (before ++ other.before).distinct,
                (after ++ other.after).distinct,
                other.description.orElse(description),
                documentation.map(_.merge(other.documentation)).orElse(other.documentation)
            )
        }
        def identifier : TargetIdentifier = TargetIdentifier(name, project.map(_.name))
    }
}


trait Target extends Instance {
    override type PropertiesType = Target.Properties

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: Category = Category.TARGET

    /**
      * Returns an identifier for this target
      * @return
      */
    def identifier : TargetIdentifier

    /**
     * Returns a description of the build target
     * @return
     */
    def description : Option[String]

    /**
     * Returns a (static) documentation of this target
     * @return
     */
    def documentation : Option[TargetDoc]

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    def digest(phase:Phase) : TargetDigest

    /**
      * Returns an explicit user defined list of targets to be executed after this target. I.e. this
      * target needs to be executed before all other targets in this list.
      * @return
      */
    def before : Seq[TargetIdentifier]

    /**
      * Returns an explicit user defined list of targets to be executed before this target I.e. this
      * * target needs to be executed after all other targets in this list.
      *
      * @return
      */
    def after : Seq[TargetIdentifier]

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    def phases : Set[Phase]

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    def provides(phase:Phase) : Set[ResourceIdentifier]

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    def requires(phase:Phase) : Set[ResourceIdentifier]

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param execution
     * @param phase
     * @return
     */
    def dirty(execution: Execution, phase: Phase) : Trilean

    /**
     * Executes a specific phase of this target. This method is should not throw a non-fatal exception, instead it
     * should wrap any exception in the TargetResult
     * @param execution
     * @param phase
     */
    def execute(execution: Execution, phase: Phase) : TargetResult

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    def link(linker:Linker, phase:Phase) : Unit
}




abstract class BaseTarget extends AbstractInstance with Target {
    protected override def instanceProperties : Target.Properties

    /**
     * Returns an identifier for this target
     * @return
     */
    override def identifier : TargetIdentifier = instanceProperties.identifier

    /**
     * Returns a description of the build target
     *
     * @return
     */
    override def description: Option[String] = instanceProperties.description

    /**
     * Returns a (static) documentation of this target
 *
     * @return
     */
    override def documentation : Option[TargetDoc] = instanceProperties.documentation

    /**
     * Returns an instance representing this target with the context
     * @return
     */
    override def digest(phase:Phase) : TargetDigest = {
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase
        )
    }

    /**
     * Returns an explicit user defined list of targets to be executed after this target. I.e. this
     * target needs to be executed before all other targets in this list.
     * @return
     */
    override def before : Seq[TargetIdentifier] = instanceProperties.before

    /**
     * Returns an explicit user defined list of targets to be executed before this target I.e. this
     * * target needs to be executed after all other targets in this list.
     *
     * @return
     */
    override def after : Seq[TargetIdentifier] = instanceProperties.after

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = Set()

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = Set()

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = Unknown

    /**
     * Executes a specific phase of this target. This method is should not throw a non-fatal exception, instead it
     * should wrap any exception in the TargetResult
     *
     * @param execution
     * @param phase
     */
    override def execute(execution: Execution, phase: Phase) : TargetResult = {
        phase match {
            case Phase.VALIDATE => validate2(execution)
            case Phase.CREATE => create2(execution)
            case Phase.BUILD => build2(execution)
            case Phase.VERIFY => verify2(execution)
            case Phase.TRUNCATE => truncate2(execution)
            case Phase.DESTROY => destroy2(execution)
        }
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker:Linker, phase:Phase) : Unit = {}

    /**
     * Performs validation before execution. This might be a good point in time to validate any assumption on data
     * sources. This method should not throw an exception, instead it should wrap up any exception in the
     * [[TargetResult]].
     */
    protected def validate2(execution:Execution) : TargetResult = {
        TargetResult.of(this, Phase.VALIDATE)(validate(execution))
    }

    /**
     * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
     * will not provide the data itself, it will only create the container. This method should not throw an exception,
     * instead it should wrap up any exception in the [[TargetResult]].
     * @param execution
     */
    protected def create2(execution:Execution) : TargetResult = {
        TargetResult.of(this, Phase.CREATE)(create(execution))
    }

    /**
     * Abstract method which will perform the build phase. This method should not throw an exception,
     * instead it should wrap up any exception in the [[TargetResult]].
     *
     * @param execution
     */
    protected def build2(execution:Execution) : TargetResult = {
        TargetResult.of(this, Phase.BUILD)(build(execution))
    }

    /**
     * Performs a verification of the build step or possibly other checks. This method should not throw an exception,
     * instead it should wrap up any exception in the [[TargetResult]].
     *
     * @param execution
     */
    protected def verify2(execution: Execution) : TargetResult = {
        TargetResult.of(this, Phase.VERIFY)(verify(execution))
    }

    /**
     * Deletes data of a specific target. This method should not throw an exception, instead it should wrap up any
     * exception in the [[TargetResult]].
     *
     * @param execution
     */
    protected def truncate2(execution:Execution) : TargetResult = {
        TargetResult.of(this, Phase.TRUNCATE)(truncate(execution))
    }

    /**
     * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
     * the table definition. This method should not throw an exception, instead it should wrap up any exception
     * in the [[TargetResult]].
     * @param execution
     */
    protected def destroy2(execution:Execution) : TargetResult = {
        TargetResult.of(this, Phase.DESTROY)(destroy(execution))
    }

    /**
     * Performs validation before execution. This might be a good point in time to validate any assumption on data
     * sources. This method may throw an exception, which will be caught an wrapped in the final [[TargetResult.]]
     * @param execution
     */
    protected def validate(execution:Execution) : Unit = {}

    /**
     * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
     * will not provide the data itself, it will only create the container. This method may throw an exception, which
     * will be caught an wrapped in the final [[TargetResult.]]
     * @param execution
     */
    protected def create(execution:Execution) : Unit = {}

    /**
     * Abstract method which will perform the output operation. This method may throw an exception, which will be
     * caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    protected def build(execution:Execution) : Unit = {}

    /**
     * Performs a verification of the build step or possibly other checks. . This method may throw an exception,
     * which will be caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    protected def verify(execution: Execution) : Unit = {}

    /**
     * Deletes data of a specific target. This method may throw an exception, which will be caught an wrapped in the
     * final [[TargetResult.]]
     *
     * @param execution
     */
    protected def truncate(execution:Execution) : Unit = {}

    /**
     * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
     * the table definition. This method may throw an exception, which will be caught an wrapped in the final
     * [[TargetResult.]]
     * @param execution
     */
    protected def destroy(execution:Execution) : Unit = {}

    protected def countRecords(execution:Execution, df:DataFrame, phase:Phase=Phase.BUILD) : DataFrame = {
        val registry = execution.metricSystem

        val metricName = "target_records"
        val bundle = MultiMetricBundle.forMetadata(registry, metricName, metadata, phase)
        lazy val newCounter = execution.spark.sparkContext.longAccumulator
        val metricLabels = bundle.labels ++ metadata.asMap
        val metric = bundle.getOrCreateMetric(metricName, metricLabels, LongAccumulatorMetric(metricName, metricLabels, newCounter))

        val counter = metric.counter
        count_records(df, counter)
    }
}
