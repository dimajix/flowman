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

import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.TargetIdentifier


final case class ProjectReference(
    name:String
) extends Reference {
    override def toString: String = "/project=" + name
    override def parent: Option[Reference] = None
    override def kind : String = "reference"
}


final case class ProjectDoc(
    name: String,
    version: Option[String] = None,
    description: Option[String] = None,
    targets:Map[TargetIdentifier,TargetDoc] = Map(),
    relations:Map[RelationIdentifier,RelationDoc] = Map(),
    mappings:Map[MappingIdentifier,MappingDoc] = Map()
) extends EntityDoc {
    override def reference: Reference = ProjectReference(name)
    override def parent: Option[Reference] = None
    override def fragments: Seq[Fragment] = (targets.values ++ relations.values ++ mappings.values).toSeq

    override def resolve(path:Seq[Reference]) : Option[Fragment] = {
        if (path.isEmpty)
            Some(this)
        else
            None
    }

    override def reparent(parent: Reference): ProjectDoc = ???

    def resolve(ref:Reference) : Option[Fragment] = {
        ref.path match {
            case head :: tail =>
                if (head != reference)
                    None
                else
                    resolve(head).flatMap(_.resolve(tail))
            case Nil =>
                None
        }
    }
}
