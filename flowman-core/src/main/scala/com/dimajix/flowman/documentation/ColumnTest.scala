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

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.spi.ColumnTestExecutor


final case class ColumnTestReference(
    override val parent:Option[Reference]
) extends Reference


abstract class ColumnTest extends Fragment with Product with Serializable {
    def result : Option[TestResult]
    def withResult(result:TestResult) : ColumnTest

    override def reparent(parent: Reference): ColumnTest

    override def parent: Some[Reference]
    override def reference: ColumnTestReference = ColumnTestReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
}


case class NotNullColumnTest(
    parent:Some[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest {
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnTest = {
        val ref = ColumnTestReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

case class UniqueColumnTest(
    parent:Some[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnTest = copy(parent=Some(parent))
}

case class RangeColumnTest(
    parent:Some[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnTest = copy(parent=Some(parent))
}

case class ValuesColumnTest(
    parent:Some[Reference],
    description: Option[String] = None,
    result:Option[TestResult] = None
) extends ColumnTest  {
    override def withResult(result: TestResult): ColumnTest = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnTest = copy(parent=Some(parent))
}

//case class ForeignKeyColumnTest() extends ColumnTest
//case class ExpressionColumnTest() extends ColumnTest


class DefaultColumnTestExecutor extends ColumnTestExecutor {
    override def execute(execution: Execution, df: DataFrame, column:String, test: ColumnTest): Option[TestResult] = ???
}
