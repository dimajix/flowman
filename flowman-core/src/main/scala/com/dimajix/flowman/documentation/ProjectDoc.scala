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
    targets:Seq[TargetDoc] = Seq.empty,
    relations:Seq[RelationDoc] = Seq.empty,
    mappings:Seq[MappingDoc] = Seq.empty
) extends EntityDoc {
    override def reference: Reference = ProjectReference(name)
    override def parent: Option[Reference] = None
    override def fragments: Seq[Fragment] = targets ++ relations ++ mappings

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
                else if (tail.isEmpty)
                    Some(this)
                else
                    fragments.find(_.reference == tail.head).flatMap(_.resolve(tail.tail))
            case Nil =>
                None
        }
    }
}
