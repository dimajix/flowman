/*
 * Copyright (C) 2022 The Flowman Authors
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
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


final case class TargetPhaseReference(
    override val parent:Option[Reference],
    phase:Phase
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/phase=" + phase.upper
            case None => phase.upper
        }
    }
    override def kind : String = "target_phase"
}


final case class TargetPhaseDoc(
    parent: Option[Reference],
    phase: Phase,
    description: Option[String] = None,
    provides: Seq[ResourceIdentifier] = Seq.empty,
    requires: Seq[ResourceIdentifier] = Seq.empty
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
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/target=" + name
            case None => name
        }
    }
    override def kind: String = "target"
}


object TargetDoc {
    def apply(
        parent: Option[Reference],
        target: Option[Target] = None,
        description: Option[String] = None,
        phases: Seq[TargetPhaseDoc] = Seq.empty,
        inputs: Seq[Reference] = Seq.empty,
        outputs: Seq[Reference] = Seq.empty
    ) : TargetDoc = {
        TargetDoc(
            target,
            parent,
            target.map(_.kind).getOrElse(""),
            target.map(_.identifier).getOrElse(TargetIdentifier.empty),
            description,
            phases,
            inputs,
            outputs
        )
    }
}
final case class TargetDoc(
    target: Option[Target],
    parent: Option[Reference],
    kind: String,
    identifier: TargetIdentifier,
    description: Option[String],
    phases: Seq[TargetPhaseDoc],
    inputs: Seq[Reference],
    outputs: Seq[Reference]
) extends EntityDoc {
    override def reference: TargetReference = TargetReference(parent, name)
    override def fragments: Seq[Fragment] = phases
    override def reparent(parent: Reference): TargetDoc = {
        val ref = TargetReference(Some(parent), name)
        copy(
            parent = Some(parent),
            phases = phases.map(_.reparent(ref))
        )
    }

    /**
     * Returns the name of the project of this relation
     * @return
     */
    def project: Option[String] = identifier.project

    override def category: Category = Category.TARGET
    override def name: String = identifier.name

    /**
     * Merge this schema documentation with another target documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[TargetDoc]) : TargetDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another target documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:TargetDoc) : TargetDoc = {
        val tgt = this.target.orElse(other.target)
        val id = if (this.identifier.isEmpty) other.identifier else this.identifier
        val kind = if (this.kind.isEmpty) other.kind else this.kind
        val desc = other.description.orElse(this.description)
        val in = inputs.toSet ++ other.inputs.toSet
        val out = outputs.toSet ++ other.outputs.toSet
        val result = copy(target=tgt, kind=kind, identifier=id, description=desc, inputs=in.toSeq, outputs=out.toSeq)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}
