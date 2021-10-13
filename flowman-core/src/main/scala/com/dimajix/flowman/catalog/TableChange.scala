/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import java.util.Locale

import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.SchemaUtils.coerce
import com.dimajix.flowman.types.StructType


abstract sealed class TableChange
abstract sealed class ColumnChange extends TableChange

object TableChange {
    case class ReplaceTable(schema:StructType) extends TableChange

    case class DropColumn(column:String) extends ColumnChange
    case class AddColumn(column:Field) extends ColumnChange
    case class UpdateColumnNullability(column:String, nullable:Boolean) extends ColumnChange
    case class UpdateColumnType(column:String, dataType:FieldType) extends ColumnChange
    case class UpdateColumnComment(column:String, comment:Option[String]) extends ColumnChange

    /**
     * Creates a Sequence of [[TableChange]] objects, which will transform a source schema into a target schema.
     * The specified [[MigrationPolicy]] is used to decide on a per-column basis, if a migration is required.
     * @param sourceSchema
     * @param targetSchema
     * @param migrationPolicy
     * @return
     */
    def migrate(sourceSchema:StructType, targetSchema:StructType, migrationPolicy:MigrationPolicy) : Seq[TableChange] = {
        val targetFields = targetSchema.fields.map(f => (f.name.toLowerCase(Locale.ROOT), f))
        val targetFieldsByName = targetFields.toMap
        val sourceFieldsByName = sourceSchema.fields.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap

        val dropFields = (sourceFieldsByName.keySet -- targetFieldsByName.keySet).toSeq.flatMap { fieldName =>
            if (migrationPolicy == MigrationPolicy.STRICT)
                Some(DropColumn(sourceFieldsByName(fieldName).name))
            else
                None
        }

        val changeFields = targetFields.flatMap { case(tgtName,tgtField) =>
            sourceFieldsByName.get(tgtName) match {
                case None => Seq(AddColumn(tgtField))
                case Some(srcField) =>
                    val modType =
                        if (migrationPolicy == MigrationPolicy.STRICT && srcField.ftype != tgtField.ftype)
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype))
                        else if (migrationPolicy == MigrationPolicy.RELAXED && coerce(srcField.ftype, tgtField.ftype) != srcField.ftype)
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype))
                        else
                            Seq()
                    val modNullability =
                        if (migrationPolicy == MigrationPolicy.STRICT && srcField.nullable != tgtField.nullable)
                            Seq(UpdateColumnNullability(srcField.name, tgtField.nullable))
                        else if (migrationPolicy == MigrationPolicy.RELAXED && !srcField.nullable && tgtField.nullable)
                            Seq(UpdateColumnNullability(srcField.name, tgtField.nullable))
                        else
                            Seq()
                    val modComment =
                        if (migrationPolicy == MigrationPolicy.STRICT && srcField.description != tgtField.description)
                            Seq(UpdateColumnComment(srcField.name, tgtField.description))
                        else
                            Seq()

                    modType ++ modNullability ++ modComment
            }
        }

        dropFields ++ changeFields
    }

    def requiresMigration(sourceSchema:StructType, targetSchema:StructType, migrationPolicy:MigrationPolicy) : Boolean = {
        // Ensure that current real Hive schema is compatible with specified schema
        migrationPolicy match {
            case MigrationPolicy.RELAXED =>
                val sourceFields = sourceSchema.fields.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap
                targetSchema.fields.exists { tgt =>
                    !sourceFields.get(tgt.name.toLowerCase(Locale.ROOT))
                        .exists(src => SchemaUtils.isCompatible(tgt, src))
                }
            case MigrationPolicy.STRICT =>
                SchemaUtils.normalize(sourceSchema)
                val sourceFields = SchemaUtils.normalize(sourceSchema).fields.sortBy(_.name)
                val targetFields = SchemaUtils.normalize(targetSchema).fields.sortBy(_.name)
                sourceFields != targetFields
        }
    }
}
