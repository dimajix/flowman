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

import scala.annotation.tailrec

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StructType


final case class SchemaReference(
    override val parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/schema"
            case None => "schema"
        }
    }
    override def kind : String = "schema"

    def sql : String = {
        parent match {
            case Some(rel:RelationReference) => rel.sql
            case Some(map:MappingOutputReference) => map.sql
            case _ => ""
        }
    }
}


object SchemaDoc {
    def ofStruct(parent:Reference, struct:StructType) : SchemaDoc = ofFields(parent, struct.fields)
    def ofFields(parent:Reference, fields:Seq[Field]) : SchemaDoc = {
        val doc = SchemaDoc(Some(parent))

        def genColumns(parent:Reference, fields:Seq[Field]) : Seq[ColumnDoc] = {
            fields.zipWithIndex.map { case(f,idx) => genColumn(parent, f, idx + 1) }
        }
        @tailrec
        def genChildren(parent:Reference, ftype:FieldType) : Seq[ColumnDoc] = {
            ftype match {
                case s:StructType =>
                    genColumns(parent, s.fields)
                case m:MapType =>
                    genChildren(parent, m.valueType)
                case a:ArrayType =>
                    genChildren(parent, a.elementType)
                case _ =>
                    Seq.empty
            }

        }
        def genColumn(parent:Reference, field:Field, idx:Int) : ColumnDoc = {
            val doc = ColumnDoc(Some(parent), field, index=idx)
            val childs = genChildren(doc.reference, field.ftype)
            doc.copy(children = childs)
        }
        val columns = genColumns(doc.reference, fields)
        doc.copy(columns = columns)
    }
}


final case class SchemaDoc(
    parent:Option[Reference],
    description:Option[String] = None,
    columns:Seq[ColumnDoc] = Seq.empty,
    checks:Seq[SchemaCheck] = Seq.empty
) extends EntityDoc {
    override def reference: SchemaReference = SchemaReference(parent)
    override def fragments: Seq[Fragment] = columns ++ checks
    override def reparent(parent: Reference): SchemaDoc = {
        val ref = SchemaReference(Some(parent))
        copy(
            parent = Some(parent),
            columns = columns.map(_.reparent(ref)),
            checks = checks.map(_.reparent(ref))
        )
    }

    /**
     * Convert this schema documentation to a Flowman struct
     */
    def toStruct : StructType = StructType(columns.map(_.field))

    /**
     * Merge this schema documentation with another schema documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[SchemaDoc]) : SchemaDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another schema documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:SchemaDoc) : SchemaDoc = {
        val desc = other.description.orElse(this.description)
        val tsts = checks ++ other.checks
        val cols = ColumnDoc.merge(columns, other.columns)
        val result = copy(description=desc, columns=cols, checks=tsts)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }

    /**
     * Enrich a Flowman struct with information from schema documentation
     * @param schema
     * @return
     */
    def enrich(schema:StructType) : StructType = {
        def enrichStruct(columns:Seq[ColumnDoc], struct:StructType) : StructType = {
            val columnsByName = MapIgnoreCase(columns.map(c => c.name -> c))
            val fields = struct.fields.map(f => columnsByName.get(f.name).map(_.enrich(f)).getOrElse(f))
            struct.copy(fields = fields)
        }
        enrichStruct(columns, schema)
    }
}
