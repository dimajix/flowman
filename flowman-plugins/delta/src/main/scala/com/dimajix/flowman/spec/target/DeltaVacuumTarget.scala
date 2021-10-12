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

package com.dimajix.flowman.spec.target

import java.time.Duration

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.relation.DeltaFileRelation
import com.dimajix.flowman.spec.relation.DeltaRelation
import com.dimajix.flowman.spec.relation.DeltaTableRelation
import com.dimajix.flowman.spec.relation.RelationReferenceSpec


case class DeltaVacuumTarget(
    override val instanceProperties:Target.Properties,
    relation:Reference[Relation],
    retentionTime:Option[Duration] = None
) extends BaseTarget {
    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases: Set[Phase] = Set(Phase.BUILD)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        val rel = relation.value
        rel.provides ++ rel.requires
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
            case Phase.BUILD => Unknown
            case _ => No
        }
    }

    /**
     * Abstract method which will perform the output operation. This method may throw an exception, which will be
     * caught an wrapped in the final [[TargetResult]].
     *
     * @param execution
     */
    override protected def build(execution: Execution): Unit = {
        val rel = relation.value.asInstanceOf[DeltaRelation]
        val deltaTable = rel match {
            case table:DeltaTableRelation => DeltaTable.forName(execution.spark, TableIdentifier(table.table, Some(table.database)).toString())
            case files:DeltaFileRelation => DeltaTable.forPath(execution.spark, files.location.toString)
            case _ => throw new UnsupportedOperationException("DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles")
        }

        retentionTime match {
            case Some(duration) => deltaTable.vacuum(duration.toHours)
            case None => deltaTable.vacuum()
        }
    }
}


@TargetType(kind="deltaVacuum")
class DeltaVacuumTargetSpec extends TargetSpec {
    @JsonProperty(value = "relation", required = true) private var relation: RelationReferenceSpec = _
    @JsonProperty(value = "retentionTime", required = true) private var retentionTime: Option[String] = None

    override def instantiate(context: Context): DeltaVacuumTarget = {
        DeltaVacuumTarget(
            instanceProperties(context),
            relation.instantiate(context),
            context.evaluate(retentionTime).map(Duration.parse)
        )
    }
}
