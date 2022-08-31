/*
 * Copyright 2022 Kaya Kupferschmidt
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

import java.sql.Statement

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.target.JdbcCommandTarget.Action
import com.dimajix.flowman.spec.target.JdbcCommandTargetSpec.ActionSpec

object JdbcCommandTarget {
    case class Action(
        sql: String,
        condition:Option[String] = None
    )
}

case class JdbcCommandTarget (
    instanceProperties: Target.Properties,
    connection: Reference[Connection],
    properties: Map[String,String] = Map.empty,
    validateAction:Option[JdbcCommandTarget.Action] = None,
    createAction:Option[JdbcCommandTarget.Action] = None,
    buildAction:Option[JdbcCommandTarget.Action] = None,
    verifyAction:Option[JdbcCommandTarget.Action] = None,
    truncateAction:Option[JdbcCommandTarget.Action] = None,
    destroyAction:Option[JdbcCommandTarget.Action] = None
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[JdbcCommandTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases: Set[Phase] = {
        validateAction.map(_ => Phase.VALIDATE).toSet ++
            createAction.map(_ => Phase.CREATE).toSet ++
            buildAction.map(_ => Phase.BUILD).toSet ++
            verifyAction.map(_ => Phase.VERIFY).toSet ++
            truncateAction.map(_ => Phase.TRUNCATE).toSet ++
            destroyAction.map(_ => Phase.DESTROY).toSet
    }

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = {
        Set.empty
    }

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        Set.empty
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
            case Phase.VALIDATE => isDirty(validateAction, Phase.VALIDATE)
            case Phase.CREATE => isDirty(createAction, Phase.CREATE)
            case Phase.BUILD => isDirty(buildAction, Phase.BUILD)
            case Phase.VERIFY => isDirty(verifyAction, Phase.VERIFY)
            case Phase.TRUNCATE => isDirty(truncateAction, Phase.TRUNCATE)
            case Phase.DESTROY => isDirty(destroyAction, Phase.DESTROY)
        }
    }
    private def isDirty(action:Option[Action], phase:Phase) : Trilean = {
        action.map(a => isDirty(a, phase)).getOrElse(No)
    }
    private def isDirty(action:Action, phase:Phase) : Trilean = {
        action.condition.map { c =>
            withStatement { stmt =>
                try {
                    val result = stmt.executeQuery(c)
                    try {
                        if (result.next()) {
                            if (result.getInt(1) >= 1)
                                Yes
                            else
                                No
                        }
                        else {
                            // Empty result => Not dirty
                            No
                        }
                    }
                    finally {
                        result.close()
                    }
                }
                catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Cannot determine dirty status of target '$identifier' in phase $phase because of exception during query execution:\n  ${reasons(ex)}")
                        Unknown
                }
            }
        }.getOrElse(Yes)
    }

    /**
     * Performs validation before execution. This might be a good point in time to validate any assumption on data
     * sources. This method may throw an exception, which will be caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    override def validate(execution: Execution): Unit = {
        executeAction(validateAction, Phase.VALIDATE)
    }

    /**
     * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
     * will not provide the data itself, it will only create the container. This method may throw an exception, which
     * will be caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    override def create(execution: Execution): Unit = {
        executeAction(createAction, Phase.CREATE)
    }

    /**
     * Abstract method which will perform the output operation. This method may throw an exception, which will be
     * caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    override def build(execution: Execution): Unit = {
        executeAction(buildAction, Phase.BUILD)
    }

    /**
     * Performs a verification of the build step or possibly other checks. . This method may throw an exception,
     * which will be caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    override def verify(execution: Execution): Unit = {
        executeAction(verifyAction, Phase.VERIFY)
    }

    /**
     * Deletes data of a specific target. This method may throw an exception, which will be caught an wrapped in the
     * final [[TargetResult.]]
     *
     * @param execution
     */
    override def truncate(execution: Execution): Unit = {
        executeAction(truncateAction, Phase.TRUNCATE)
    }

    /**
     * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
     * the table definition. This method may throw an exception, which will be caught an wrapped in the final
     * [[TargetResult.]]
     *
     * @param execution
     */
    override def destroy(execution: Execution): Unit = {
        executeAction(destroyAction, Phase.DESTROY)
    }

    private def executeAction(action:Option[Action], phase:Phase) : Unit = {
        action.foreach { a =>
            val dirty = isDirty(a, phase)
            if (dirty == Yes || dirty == Unknown) {
                logger.info(s"Executing phase $phase for jdbcCommand '$identifier'")
                withStatement { stmt =>
                    stmt.executeUpdate(a.sql)
                }
            }
        }
    }
    private def withConnection[T](fn: (java.sql.Connection, JDBCOptions) => T): T = {
        val connection = this.connection.value.asInstanceOf[JdbcConnection]
        val props = connection.toConnectionProperties() ++ properties + ("query" -> "dummy")
        val options = new JDBCOptions(props)

        JdbcUtils.withConnection(options) { con => fn(con, options) }
    }

    private def withStatement[T](fn: Statement => T): T = {
        withConnection { (con, options) =>
            val statement = con.createStatement()
            try {
                statement.setQueryTimeout(JdbcUtils.queryTimeout(options))
                fn(statement)
            }
            finally {
                statement.close()
            }
        }
    }
}


object JdbcCommandTargetSpec {
    class ActionSpec {
        @JsonProperty(value="sql", required=true) private var sql:String = _
        @JsonProperty(value="condition", required=false) private var condition:Option[String] = None

        def instantiate(context: Context) : JdbcCommandTarget.Action = {
            JdbcCommandTarget.Action(
                context.evaluate(sql),
                context.evaluate(condition)
            )
        }
    }
}
class JdbcCommandTargetSpec extends TargetSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value="validate", required=true) private var validate:Option[ActionSpec] = None
    @JsonProperty(value="create", required=true) private var create:Option[ActionSpec] = None
    @JsonProperty(value="build", required=true) private var build:Option[ActionSpec] = None
    @JsonProperty(value="verify", required=true) private var verify:Option[ActionSpec] = None
    @JsonProperty(value="truncate", required=true) private var truncate:Option[ActionSpec] = None
    @JsonProperty(value="destroy", required=true) private var destroy:Option[ActionSpec] = None

    override def instantiate(context: Context, props:Option[Target.Properties] = None): JdbcCommandTarget = {
        JdbcCommandTarget(
            instanceProperties(context, props),
            connection.instantiate(context),
            context.evaluate(properties),
            validate.map(_.instantiate(context)),
            create.map(_.instantiate(context)),
            build.map(_.instantiate(context)),
            verify.map(_.instantiate(context)),
            truncate.map(_.instantiate(context)),
            destroy.map(_.instantiate(context)),
        )
    }
}
