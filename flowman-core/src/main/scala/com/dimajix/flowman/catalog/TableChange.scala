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

package com.dimajix.flowman.catalog

import java.util.Locale

import scala.collection.mutable

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.SchemaUtils.coerce
import com.dimajix.flowman.types.StructType


abstract sealed class TableChange extends Product with Serializable
abstract sealed class ColumnChange extends TableChange
abstract sealed class IndexChange extends TableChange

object TableChange {
    case class ReplaceTable(schema:StructType) extends TableChange

    case class DropColumn(column:String) extends ColumnChange
    case class AddColumn(column:Field) extends ColumnChange
    case class UpdateColumnNullability(column:String, nullable:Boolean) extends ColumnChange
    case class UpdateColumnType(column:String, dataType:FieldType) extends ColumnChange
    case class UpdateColumnComment(column:String, comment:Option[String]) extends ColumnChange

    case class CreatePrimaryKey(columns:Seq[String]) extends IndexChange
    case class DropPrimaryKey() extends IndexChange
    case class CreateIndex(name:String, columns:Seq[String], unique:Boolean) extends IndexChange
    case class DropIndex(name:String) extends IndexChange

    /**
     * Creates a Sequence of [[TableChange]] objects, which will transform a source schema into a target schema.
     * The specified [[MigrationPolicy]] is used to decide on a per-column basis, if a migration is required.
     * @param sourceSchema
     * @param targetSchema
     * @param migrationPolicy
     * @return
     */
    def migrate(sourceTable:TableDefinition, targetTable:TableDefinition, migrationPolicy:MigrationPolicy) : Seq[TableChange] = {
        val normalizedSource = sourceTable.normalize()
        val normalizedTarget = targetTable.normalize()

        val targetFields = targetTable.columns.map(f => f.name -> f)
        val targetFieldsByName = MapIgnoreCase(targetFields)
        val sourceFieldsByName = MapIgnoreCase(sourceTable.columns.map(f => f.name -> f))

        // Check which fields need to be dropped
        val dropFields = (sourceFieldsByName.keySet -- targetFieldsByName.keySet).toSeq.flatMap { fieldName =>
            if (migrationPolicy == MigrationPolicy.STRICT)
                Some(DropColumn(sourceFieldsByName(fieldName).name))
            else
                None
        }

        // PK also needs to be recreated if data type changes (github-154)
        var changePk = normalizedSource.primaryKey != normalizedTarget.primaryKey
        // Indexes need to be recreated if the data type changes (github-156)
        val changeIndexes = mutable.Set[String]()

        // Infer column changes
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

                    // If the data type of a PK element changes, then the PK needs to recreated
                    if (modType.nonEmpty && normalizedTarget.primaryKey.contains(srcField.name.toLowerCase(Locale.ROOT)))
                        changePk = true
                    // If the data type of an index changes, then the Index needs to be recreated
                    if (modType.nonEmpty) {
                        normalizedTarget.indexes.foreach { idx =>
                            if (idx.columns.contains(srcField.name.toLowerCase(Locale.ROOT)))
                                changeIndexes.add(idx.name)
                        }
                    }

                    modType ++ modNullability ++ modComment
            }
        }

        // Check if primary key needs to be dropped
        val dropPk = if(normalizedSource.primaryKey.nonEmpty && changePk)
            Some(DropPrimaryKey())
        else
            None
        val createPk = if (normalizedTarget.primaryKey.nonEmpty && changePk)
            Some(CreatePrimaryKey(targetTable.primaryKey))
                else
            None

        // Check which Indexes need to be dropped
        val dropIndexes = sourceTable.indexes.flatMap { src =>
            targetTable.indexes.find(_.name.toLowerCase(Locale.ROOT) == src.name.toLowerCase(Locale.ROOT)) match {
                case None =>
                    Some(DropIndex(src.name))
                case Some(tgt) =>
                    if (src.normalize() != tgt.normalize() || changeIndexes.contains(src.name.toLowerCase(Locale.ROOT)))
                        Some(DropIndex(src.name))
                    else None
            }
        }
        val addIndexes = targetTable.indexes.flatMap { tgt =>
            sourceTable.indexes.find(_.name.toLowerCase(Locale.ROOT) == tgt.name.toLowerCase(Locale.ROOT)) match {
                case None =>
                    Some(CreateIndex(tgt.name, tgt.columns, tgt.unique))
                case Some(src) =>
                    if (src.normalize() != tgt.normalize() || changeIndexes.contains(src.name.toLowerCase(Locale.ROOT)))
                        Some(CreateIndex(tgt.name, tgt.columns, tgt.unique))
                    else
                        None
            }
        }

        dropIndexes ++ dropPk ++ dropFields ++ changeFields ++ createPk ++ addIndexes
    }

    /**
     * Performs a check if a migration is required
     * @param sourceTable
     * @param targetTable
     * @param migrationPolicy
     * @return
     */
    def requiresMigration(sourceTable:TableDefinition, targetTable:TableDefinition, migrationPolicy:MigrationPolicy) : Boolean = {
        val normalizedSource = sourceTable.normalize()
        val normalizedTarget = targetTable.normalize()

        // Check if PK needs change
        val pkChanges = normalizedSource.primaryKey != normalizedTarget.primaryKey

        // Check if indices need change
        val dropIndexes = !normalizedSource.indexes.forall(src =>
            normalizedTarget.indexes.contains(src)
        )
        val addIndexes = !normalizedTarget.indexes.forall(tgt =>
            normalizedSource.indexes.contains(tgt)
        )

        // Ensure that current real schema is compatible with specified schema
        val columnChanges = migrationPolicy match {
            case MigrationPolicy.RELAXED =>
                val sourceFields = sourceTable.columns.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap
                targetTable.columns.exists { tgt =>
                    !sourceFields.get(tgt.name.toLowerCase(Locale.ROOT))
                        .exists(src => SchemaUtils.isCompatible(tgt, src))
                }
            case MigrationPolicy.STRICT =>
                val sourceFields = SchemaUtils.normalize(sourceTable.columns).sortBy(_.name)
                val targetFields = SchemaUtils.normalize(targetTable.columns).sortBy(_.name)
                sourceFields != targetFields
        }

        pkChanges || dropIndexes || addIndexes || columnChanges
    }
}
