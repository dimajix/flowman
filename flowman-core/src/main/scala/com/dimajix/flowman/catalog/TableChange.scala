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
abstract sealed class PartitionChange extends TableChange
abstract sealed class IndexChange extends TableChange

object TableChange {
    case class ChangeStorageFormat(format:String) extends TableChange

    case class DropColumn(column:String) extends ColumnChange
    case class AddColumn(column:Field) extends ColumnChange
    case class UpdateColumnNullability(column:String, nullable:Boolean) extends ColumnChange
    case class UpdateColumnType(column:String, dataType:FieldType, charset:Option[String]=None, collation:Option[String]=None) extends ColumnChange
    case class UpdateColumnComment(column:String, comment:Option[String]) extends ColumnChange

    case class UpdatePartitionColumns(columns:Seq[Field]) extends PartitionChange

    case class CreatePrimaryKey(columns:Seq[String], clustered:Boolean) extends IndexChange
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

        // Check if desired storage format is different from current format
        val storageChange = {
            val changedStorage = normalizedTarget.storageFormat.exists(sf => !normalizedSource.storageFormat.contains(sf))

            if (changedStorage)
                targetTable.storageFormat.map(f => ChangeStorageFormat(f))
            else
                None
        }

        // Check if partition columns need a change
        val partitionChange = {
            val changedColumnNames = normalizedSource.partitionColumnNames.sorted != normalizedTarget.partitionColumnNames.sorted

            // Ensure that current real schema is compatible with specified schema
            val columnChanges = requiresSchemaMigration(sourceTable.partitionColumns, targetTable.partitionColumns, migrationPolicy)

            if (changedColumnNames || columnChanges)
                Some(UpdatePartitionColumns(targetTable.partitionColumns))
            else
                None
        }

        val targetFields = targetTable.dataColumns
            .filterNot(f => normalizedSource.partitionColumnNames.contains(f.name.toLowerCase(Locale.ROOT)))
            .map(f => f.name -> f)
        val targetFieldsByName = MapIgnoreCase(targetFields)
        val sourceFields = sourceTable.dataColumns
            .filterNot(f => normalizedTarget.partitionColumnNames.contains(f.name.toLowerCase(Locale.ROOT)))
            .map(f => f.name -> f)
        val sourceFieldsByName = MapIgnoreCase(sourceFields)

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
                        // Check if collation has changed
                        if (tgtField.charset.exists(c => srcField.charset.exists(_ != c)) || tgtField.collation.exists(c => srcField.collation.exists(_ != c)))
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype, tgtField.charset, tgtField.collation))
                        // Check if data type has changed
                        else if (migrationPolicy == MigrationPolicy.STRICT && srcField.ftype != tgtField.ftype)
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype, tgtField.charset, tgtField.collation))
                        else if (migrationPolicy == MigrationPolicy.RELAXED && coerce(srcField.ftype, tgtField.ftype) != srcField.ftype)
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype, tgtField.charset, tgtField.collation))
                        // If new PK contains this field, we better always update data type
                        else if (migrationPolicy == MigrationPolicy.RELAXED && normalizedTarget.primaryKey.exists(_.columns.contains(srcField.name.toLowerCase(Locale.ROOT)))
                            && srcField.ftype != tgtField.ftype)
                            Seq(UpdateColumnType(srcField.name, tgtField.ftype, tgtField.charset, tgtField.collation))
                        else
                            Seq.empty

                    // If the data type of a PK element changes, then the PK needs to recreated
                    if (modType.nonEmpty && normalizedTarget.primaryKey.exists(_.columns.contains(srcField.name.toLowerCase(Locale.ROOT))))
                        changePk = true

                    val modNullability =
                        if (migrationPolicy == MigrationPolicy.STRICT && srcField.nullable != tgtField.nullable)
                            Seq(UpdateColumnNullability(srcField.name, tgtField.nullable))
                        else if (migrationPolicy == MigrationPolicy.RELAXED && !srcField.nullable && tgtField.nullable)
                            Seq(UpdateColumnNullability(srcField.name, tgtField.nullable))
                        // If new PK contains this field, we also might be required to update nullability
                        else if (migrationPolicy == MigrationPolicy.RELAXED && normalizedTarget.primaryKey.exists(_.columns.contains(srcField.name.toLowerCase(Locale.ROOT)))
                            && changePk && srcField.nullable && !tgtField.nullable)
                            Seq(UpdateColumnNullability(srcField.name, tgtField.nullable))
                        else
                            Seq.empty
                    val modComment =
                        if (migrationPolicy == MigrationPolicy.STRICT && srcField.description != tgtField.description)
                            Seq(UpdateColumnComment(srcField.name, tgtField.description))
                        else
                            Seq.empty

                    // If the data type of an index changes, then the Index needs to be recreated
                    if (modType.nonEmpty || modNullability.nonEmpty) {
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
        val createPk = normalizedTarget.primaryKey
            .filter(_.columns.nonEmpty && changePk)
            .map(pk => CreatePrimaryKey(pk.columns, pk.clustered))

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

        partitionChange.toSeq ++ dropIndexes ++ dropPk ++ storageChange.toSeq ++ dropFields ++ changeFields ++ createPk ++ addIndexes
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

        // Check if desired storage format is different from current format
        val storageChanges = normalizedTarget.storageFormat.exists(sf => !normalizedSource.storageFormat.contains(sf))

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
        val columnChanges = requiresSchemaMigration(sourceTable.columns, targetTable.columns, migrationPolicy)

        // Check if partition columns require a change
        val partitionChanges = normalizedSource.partitionColumnNames.sorted != normalizedTarget.partitionColumnNames.sorted

        storageChanges || pkChanges || dropIndexes || addIndexes || columnChanges || partitionChanges
    }

    private def requiresSchemaMigration(sourceColumns:Seq[Field], targetColumns:Seq[Field], migrationPolicy:MigrationPolicy) : Boolean = {
        migrationPolicy match {
            case MigrationPolicy.RELAXED =>
                val sourceFields = SchemaUtils.normalize(sourceColumns).map(f => f.name -> f).toMap
                targetColumns.exists { tgt =>
                    sourceFields.get(tgt.name.toLowerCase(Locale.ROOT))
                        .forall(src => requiresMigrationRelaxed(src, tgt))
                }
            case MigrationPolicy.STRICT =>
                val sourceFields = SchemaUtils.normalize(sourceColumns).sortBy(_.name)
                val targetFields = SchemaUtils.normalize(targetColumns).sortBy(_.name)
                sourceFields.length != targetFields.length ||
                    sourceFields.zip(targetFields).exists { case (s,t) => requiresMigrationStrict(s,t) }
        }
    }

    private def requiresMigrationRelaxed(sourceField:Field, targetField:Field) : Boolean = {
        !SchemaUtils.isCompatible(targetField, sourceField)
    }

    private def requiresMigrationStrict(sourceField:Field, targetField:Field) : Boolean = {
        sourceField.copy(charset=None, collation=None) != targetField.copy(charset=None, collation=None) ||
            targetField.charset.exists(c => sourceField.charset.exists(_ != c)) ||
            targetField.collation.exists(c => sourceField.collation.exists(_ != c))
    }
}
