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
import com.dimajix.flowman.spi.SchemaTestExecutor


final case class SchemaTestReference(
    override val parent:Option[SchemaReference]
) extends Reference


sealed abstract class SchemaTest extends Fragment with Product with Serializable {
    def result : Option[TestResult]
    def withResult(result:TestResult) : SchemaTest

    override def parent: Option[SchemaReference]
    override def reference: SchemaTestReference = SchemaTestReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
    override def reparent(parent: Reference): SchemaTest
}


//case class ExpressionSchemaTest(
//) extends SchemaTest


class DefaultSchemaTestExecutor extends SchemaTestExecutor {
    override def execute(execution: Execution, df: DataFrame, test: SchemaTest): Option[TestResult] = ???
}
