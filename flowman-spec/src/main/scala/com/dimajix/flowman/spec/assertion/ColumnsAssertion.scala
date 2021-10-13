/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.assertion

import scala.util.parsing.combinator.RegexParsers

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.BaseAssertion
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier


object ColumnsAssertion {
    sealed abstract class Predicate {
        def description : String
        def execute(df:DataFrame) : Boolean
    }
    case class ColumnIsPresent(column:String) extends Predicate {
        override def description: String = s"$column IS PRESENT"
        override def execute(df:DataFrame) : Boolean = {
            df.columns.exists(_.equalsIgnoreCase(column))
        }
    }
    case class ColumnIsAbsent(column:String) extends Predicate {
        override def description: String = s"$column IS ABSENT"
        override def execute(df:DataFrame) : Boolean = {
            !df.columns.exists(_.equalsIgnoreCase(column))
        }
    }
    case class ColumnIsOfType(column:String, dataTypes:Seq[DataType]) extends Predicate {
        override def description: String = {
            val types =
                if (dataTypes.size == 1)
                    dataTypes.head.sql
                else dataTypes.map(_.sql).mkString("(",",",")")

            s"$column IS OF TYPE $types"
        }
        override def execute(df:DataFrame) : Boolean = {
            df.schema.fields.find(_.name.equalsIgnoreCase(column))
                .exists(f => dataTypes.contains(f.dataType))
        }
    }
}
case class ColumnsAssertion(
    override val instanceProperties:Assertion.Properties,
    mapping:MappingOutputIdentifier,
    expected:Seq[ColumnsAssertion.Predicate]
) extends BaseAssertion {
    /**
      * Returns a list of physical resources required by this assertion. This list will only be non-empty for assertions
      * which actually read from physical data.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] = Seq(mapping)

    /**
      * Executes this [[Assertion]] and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): AssertionResult = {
        require(execution != null)
        require(input != null)

        AssertionResult.of(this) {
            val df = input(mapping)

            expected.map { test =>
                AssertionTestResult.of(test.description, None) {
                    test.execute(df)
                }
            }
        }
    }
}


object ColumnsAssertionSpec {
    object PredicateParser extends RegexParsers {
        import ColumnsAssertion._

        implicit class InsensitiveString(str: String) {
            def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toUpperCase }
        }

        private val IS      = "IS".ignoreCase
        private val OF      = "OF".ignoreCase
        private val TYPE    = "TYPE".ignoreCase
        private val PRESENT = "PRESENT".ignoreCase
        private val ABSENT  = "ABSENT".ignoreCase

        private val identifier = """(^[a-zA-Z][a-zA-Z0-9_.]*)""".r

        private def columnIsPresent = identifier <~ IS <~ PRESENT ^^ { case id =>  ColumnIsPresent(id) }
        private def columnIsAbsent = identifier <~ IS <~ ABSENT ^^ { case id =>  ColumnIsAbsent(id) }
        private def columnIsOfType = identifier ~ (IS ~> OF ~> TYPE ~> identifier) ^^ { case col ~ dtype =>  ColumnIsOfType(col, Seq(CatalystSqlParser.parseDataType(dtype))) }
        private def columnIsOfTypes = identifier ~ (IS ~> OF ~> TYPE ~> "(" ~> repsep(identifier,",") <~ ")") ^^ { case col ~ dtypes =>  ColumnIsOfType(col, dtypes.map(CatalystSqlParser.parseDataType)) }

        private def commands =
            columnIsPresent | columnIsAbsent | columnIsOfType | columnIsOfTypes

        def parse(input: String): Predicate =
            parse(commands, input) match {
                case Success(res, _) => res
                case e@Error(_, _)   => throw new IllegalArgumentException(s"Cannot parse column expectation: $input\n${e.toString()}")
                case f@Failure(_, _) => throw new IllegalArgumentException(s"Cannot parse column expectation: $input\n${f.toString()}")
            }
    }
}
class ColumnsAssertionSpec extends AssertionSpec {
    import ColumnsAssertionSpec.PredicateParser

    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="expected", required=false) private var expected:Seq[String] = Seq()

    override def instantiate(context: Context): ColumnsAssertion = {
        ColumnsAssertion(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(mapping)),
            expected.map(e => PredicateParser.parse(context.evaluate(e)))
        )
    }
}
