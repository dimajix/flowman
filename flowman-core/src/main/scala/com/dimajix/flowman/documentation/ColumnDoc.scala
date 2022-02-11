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

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.NullType



final case class ColumnReference(
    override val parent:Option[Reference],
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(col:ColumnReference) => col.toString + "." + name
            case Some(ref) => ref.toString + "/column=" + name
            case None => name
        }
    }
}


object ColumnDoc {
    def merge(thisCols:Seq[ColumnDoc], otherCols:Seq[ColumnDoc]) :Seq[ColumnDoc] = {
        val thisColsByName = MapIgnoreCase(thisCols.map(c => c.name -> c))
        val otherColsByName = MapIgnoreCase(otherCols.map(c => c.name -> c))
        val mergedColumns = thisCols.map { column =>
            column.merge(otherColsByName.get(column.name))
        }
        mergedColumns ++ otherCols.filter(c => !thisColsByName.contains(c.name))
    }
}
final case class ColumnDoc(
    parent:Option[Reference],
    field:Field,
    children:Seq[ColumnDoc] = Seq(),
    tests:Seq[ColumnTest] = Seq()
) extends EntityDoc {
    override def reference: ColumnReference = ColumnReference(parent, name)
    override def fragments: Seq[Fragment] = children
    override def reparent(parent: Reference): ColumnDoc = {
        val ref = ColumnReference(Some(parent), name)
        copy(
            parent = Some(parent),
            children = children.map(_.reparent(ref)),
            tests = tests.map(_.reparent(ref))
        )
    }

    def name : String = field.name
    def description : Option[String] = field.description
    def nullable : Boolean = field.nullable
    def typeName : String = field.typeName
    def sqlType : String = field.sqlType
    def sparkType : String = field.sparkType.sql
    def catalogType : String = field.catalogType.sql

    /**
     * Merge this schema documentation with another column documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[ColumnDoc]) : ColumnDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another column documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:ColumnDoc) : ColumnDoc = {
        val childs =
            if (this.children.nonEmpty && other.children.nonEmpty)
                ColumnDoc.merge(children, other.children)
            else
                this.children ++ other.children
        val desc = other.description.orElse(description)
        val tsts = tests ++ other.tests
        val ftyp = if (field.ftype == NullType) other.field.ftype else field.ftype
        val nll = if (field.ftype == NullType) other.field.nullable else field.nullable
        val fld = field.copy(ftype=ftyp, nullable=nll, description=desc)
        copy(field=fld, children=childs, tests=tsts)
    }

    /**
     * Enriches a Flowman [[Field]] with documentation
     */
    def enrich(field:Field) : Field = {
        val desc = description.filter(_.nonEmpty).orElse(field.description)
        field.copy(description = desc)
    }
}
