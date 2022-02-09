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

package com.dimajix.flowman.documentation

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.TargetIdentifier


final case class TargetPhaseReference(
    override val parent:Option[Reference],
    phase:Phase
) extends Reference


final case class TargetPhaseDoc(
    parent:Option[Reference],
    phase:Phase,
    description:Option[String],
    provides:Seq[ResourceIdentifier],
    requires:Seq[ResourceIdentifier]
) extends Fragment {
    override def reference: Reference = TargetPhaseReference(parent, phase)
    override def fragments: Seq[Fragment] = Seq()
    override def reparent(parent: Reference): TargetPhaseDoc = {
        copy(parent = Some(parent))
    }
}


final case class TargetReference(
    override val parent:Option[Reference],
    name:String
) extends Reference


final case class TargetDoc(
    parent:Option[Reference],
    identifier:TargetIdentifier,
    description:Option[String],
    phases:Seq[TargetPhaseDoc],
    inputs:Seq[Reference],
    outputs:Seq[Reference]
) extends EntityDoc {
    override def reference: TargetReference = TargetReference(parent, identifier.name)
    override def fragments: Seq[Fragment] = phases
    override def reparent(parent: Reference): TargetDoc = {
        val ref = TargetReference(Some(parent), identifier.name)
        copy(
            parent = Some(parent),
            phases = phases.map(_.reparent(ref))
        )
    }

    def merge(other:Option[TargetDoc]) : TargetDoc = other.map(merge).getOrElse(this)
    def merge(other:TargetDoc) : TargetDoc = {
        val id = if (identifier.isEmpty) other.identifier else identifier
        val desc = other.description.orElse(this.description)
        val in = inputs.toSet ++ other.inputs.toSet
        val out = outputs.toSet ++ other.outputs.toSet
        val result = copy(identifier=id, description=desc, inputs=in.toSeq, outputs=out.toSeq)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}
