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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.BooleanType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spi.SchemaTestExecutor


final case class SchemaTestReference(
    override val parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/test"
            case None => ""
        }
    }
    override def kind : String = "schema_test"
}


abstract class SchemaTest extends Fragment with Product with Serializable {
    def name : String
    def result : Option[TestResult]
    def withResult(result:TestResult) : SchemaTest

    override def parent: Option[Reference]
    override def reference: SchemaTestReference = SchemaTestReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
    override def reparent(parent: Reference): SchemaTest
}

final case class PrimaryKeySchemaTest(
    parent:Option[Reference],
    description: Option[String] = None,
    columns:Seq[String] = Seq.empty,
    result:Option[TestResult] = None
) extends SchemaTest {
    override def name : String = s"PRIMARY KEY (${columns.mkString(",")})"
    override def withResult(result: TestResult): SchemaTest = copy(result=Some(result))
    override def reparent(parent: Reference): PrimaryKeySchemaTest = {
        val ref = SchemaTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ForeignKeySchemaTest(
    parent:Option[Reference],
    description: Option[String] = None,
    columns: Seq[String] = Seq.empty,
    relation: Option[RelationIdentifier] = None,
    mapping: Option[MappingOutputIdentifier] = None,
    references: Seq[String] = Seq.empty,
    result:Option[TestResult] = None
) extends SchemaTest {
    override def name : String = {
        val otherEntity = relation.map(_.toString).orElse(mapping.map(_.toString)).getOrElse("")
        val otherColumns = if (references.isEmpty) columns else references
        s"FOREIGN KEY (${columns.mkString(",")}) REFERENCES ${otherEntity}(${otherColumns.mkString(",")})"
    }
    override def withResult(result: TestResult): SchemaTest = copy(result=Some(result))
    override def reparent(parent: Reference): ForeignKeySchemaTest = {
        val ref = SchemaTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ExpressionSchemaTest(
    parent:Option[Reference],
    description: Option[String] = None,
    expression: String,
    result:Option[TestResult] = None
) extends SchemaTest {
    override def name: String = expression
    override def withResult(result: TestResult): SchemaTest = copy(result=Some(result))
    override def reparent(parent: Reference): ExpressionSchemaTest = {
        val ref = SchemaTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}


class DefaultSchemaTestExecutor extends SchemaTestExecutor {
    override def execute(execution: Execution, context:Context, df: DataFrame, test: SchemaTest): Option[TestResult] = {
        test match {
            case p:PrimaryKeySchemaTest =>
                val cols = p.columns.map(df(_))
                val agg = df.filter(cols.map(_.isNotNull).reduce(_ || _)).groupBy(cols:_*).count()
                val result = agg.groupBy(agg(agg.columns(cols.length)) > 1).count().collect()
                val numSuccess = result.find(_.getBoolean(0) == false).map(_.getLong(1)).getOrElse(0L)
                val numFailed = result.find(_.getBoolean(0) == true).map(_.getLong(1)).getOrElse(0L)
                val status = if (numFailed > 0) TestStatus.FAILED else TestStatus.SUCCESS
                val description = s"$numSuccess keys are unique, $numFailed keys are non-unique"
                Some(TestResult(Some(test.reference), status, Some(description)))

            case f:ForeignKeySchemaTest =>
                val otherDf =
                    f.relation.map { rel =>
                        val relation = context.getRelation(rel)
                        relation.read(execution)
                    }.orElse(f.mapping.map { map=>
                        val mapping = context.getMapping(map.mapping)
                        execution.instantiate(mapping, map.output)
                    }).getOrElse(throw new IllegalArgumentException(s"Need either mapping or relation in foreignKey test ${test.reference.toString}"))
                val cols = f.columns.map(df(_))
                val otherCols =
                    if (f.references.nonEmpty)
                        f.references.map(otherDf(_))
                    else
                        f.columns.map(otherDf(_))
                val joined = df.join(otherDf, cols.zip(otherCols).map(lr => lr._1 === lr._2).reduce(_ && _), "left")
                executePredicateTest(joined, test, otherCols.map(_.isNotNull).reduce(_ || _))

            case e:ExpressionSchemaTest =>
                executePredicateTest(df, test, expr(e.expression).cast(BooleanType))

            case _ => None
        }
    }

    private def executePredicateTest(df: DataFrame, test:SchemaTest, predicate:Column) : Option[TestResult] = {
        val result = df.groupBy(predicate).count().collect()
        val numSuccess = result.find(_.getBoolean(0) == true).map(_.getLong(1)).getOrElse(0L)
        val numFailed = result.find(_.getBoolean(0) == false).map(_.getLong(1)).getOrElse(0L)
        val status = if (numFailed > 0) TestStatus.FAILED else TestStatus.SUCCESS
        val description = s"$numSuccess records passed, $numFailed records failed"
        Some(TestResult(Some(test.reference), status, Some(description)))
    }
}
