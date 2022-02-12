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
import org.apache.spark.sql.functions.lit

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.spi.ColumnTestExecutor


final case class ColumnTestReference(
    override val parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/test"
            case None => ""
        }
    }
}


abstract class ColumnTest extends Fragment with Product with Serializable {
    def name : String
    def result : Option[TestResult]
    def withResult(result:TestResult) : ColumnTest

    override def reparent(parent: Reference): ColumnTest

    override def parent: Option[Reference]
    override def reference: ColumnTestReference = ColumnTestReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
}


final case class NotNullColumnTest(
    parent:Option[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest {
    override def name : String = "IS NOT NULL"
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnTest = {
        val ref = ColumnTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class UniqueColumnTest(
    parent:Option[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def name : String = "HAS UNIQUE VALUES"
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): UniqueColumnTest = {
        val ref = ColumnTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class RangeColumnTest(
    parent:Option[Reference],
    description: Option[String] = None,
    lower:Any,
    upper:Any,
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def name : String = s"IS BETWEEN $lower AND $upper"
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): RangeColumnTest = {
        val ref = ColumnTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ValuesColumnTest(
    parent:Option[Reference],
    description: Option[String] = None,
    values: Seq[Any] = Seq(),
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def name : String = s"IS IN (${values.mkString(",")})"
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ValuesColumnTest = {
        val ref = ColumnTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

//case class ForeignKeyColumnTest() extends ColumnTest
//case class ExpressionColumnTest() extends ColumnTest


class DefaultColumnTestExecutor extends ColumnTestExecutor {
    override def execute(execution: Execution, df: DataFrame, column:String, test: ColumnTest): Option[TestResult] = {
        test match {
            case _: NotNullColumnTest =>
                executePredicateTest(df, column, test, df(column).isNotNull)
            case _: UniqueColumnTest =>
                val agg = df.filter(df(column).isNotNull).groupBy(df(column)).count()
                val result = agg.filter(agg(agg.columns(1)) > 1).orderBy(agg(agg.columns(1)).desc).limit(6).collect()
                val status = if (result.isEmpty) TestStatus.SUCCESS else TestStatus.FAILED
                Some(TestResult(Some(test.reference), status, None, None))
            case v: ValuesColumnTest =>
                val dt = df.schema(column).dataType
                val values = v.values.map(v => lit(v).cast(dt))
                executePredicateTest(df.filter(df(column).isNotNull), column, test, df(column).isin(values:_*))
            case v: RangeColumnTest =>
                val dt = df.schema(column).dataType
                val lower = lit(v.lower).cast(dt)
                val upper = lit(v.upper).cast(dt)
                executePredicateTest(df.filter(df(column).isNotNull), column, test, df(column).between(lower, upper))
            case _ => None
        }
    }

    private def executePredicateTest(df: DataFrame, column:String, test:ColumnTest, predicate:Column) : Option[TestResult] = {
        val result = df.groupBy(predicate).count().collect()
        val numSuccess = result.find(_.getBoolean(0) == true).map(_.getLong(1)).getOrElse(0L)
        val numFailed = result.find(_.getBoolean(0) == false).map(_.getLong(1)).getOrElse(0L)
        val status = if (numFailed > 0) TestStatus.FAILED else TestStatus.SUCCESS
        Some(TestResult(Some(test.reference), status, None, None))
    }
}
