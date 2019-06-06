/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


case class HiveUnionViewRelation(
    instanceProperties:Relation.Properties,
    override val database: String,
    override val table: String,
    input: Seq[RelationIdentifier],
    override val schema:Schema,
    override val partitions: Seq[PartitionField]
) extends HiveRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveUnionViewRelation])

    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = ???

    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {
        def castField(src:Field, dst:Field) = {
            if (src.ftype == dst.ftype)
                src.name
            else
                s"CAST(${src.name} AS ${dst.ftype.sqlType}) AS ${dst.name}"
        }

        def nullField(dst:Field) = {
            s"CAST(NULL AS ${dst.ftype.sqlType}) AS ${dst.name}"
        }

        val relations = input.map(id => context.getRelation(id).asInstanceOf[HiveRelation])

        val selects = relations.map { rel =>
            val srcFields = rel.schema.fields.map(f => f.name -> f).toMap
            val resultFields = schema.fields.map { field =>
                srcFields.get(field.name).map(f => castField(f, field)).getOrElse(nullField(field))
            }
            "SELECT\n" +
                resultFields.mkString("    ",",\n    ","\n") +
            "FROM " + rel.tableIdentifier
        }

        val sql =
            s"""
               |CREATE VIEW $tableIdentifier AS
               |${selects.mkString("\n\nUNION ALL\n\n")}
             """.stripMargin

        println(sql)
        executor.spark.sql(sql)
    }

    override def destroy(executor:Executor, ifExists:Boolean=false) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???
}



class HiveUnionViewRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec{
    @JsonProperty(value="database") private var database: String = _
    @JsonProperty(value="view") private var view: String = _
    @JsonProperty(value="input") private var input: Seq[String] = Seq()

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): HiveUnionViewRelation = {
        HiveUnionViewRelation(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(view),
            input.map(i => RelationIdentifier(context.evaluate(i))),
            if (schema != null) schema.instantiate(context) else null,
            partitions.map(_.instantiate(context))
        )
    }
}
