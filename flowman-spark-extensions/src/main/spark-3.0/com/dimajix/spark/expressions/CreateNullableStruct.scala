/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.spark.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.expressions.EmptyRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamePlaceholder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


/**
  * Returns a Row containing the evaluation of all children expressions.
  */
object CreateNullableStruct extends FunctionBuilder {
    def apply(children: Seq[Expression]): CreateNullableNamedStruct = {
        CreateNullableNamedStruct(children.zipWithIndex.flatMap {
            //case (e: NamedExpression, _) if e.resolved => Seq(Literal(e.name), e)
            case (e: NamedExpression, _) if e.name.nonEmpty => Seq(Literal(e.name), e)
            case (e: NamedExpression, _) => Seq(NamePlaceholder, e)
            case (e, index) => Seq(Literal(s"col${index + 1}"), e)
        })
    }
}

/**
  * Creates a struct with the given field names and values
  *
  * @param children Seq(name1, val1, name2, val2, ...)
  */
// scalastyle:off line.size.limit
@ExpressionDescription(
    usage = "_FUNC_(name1, val1, name2, val2, ...) - Creates a struct with the given field names and values.",
    examples = """
    Examples:
      > SELECT _FUNC_("a", 1, "b", 2, "c", 3);
       {"a":1,"b":2,"c":3}
  """)
// scalastyle:on line.size.limit
case class CreateNullableNamedStruct(override val children: Seq[Expression]) extends Expression {
    lazy val (nameExprs, valExprs) = children.grouped(2).map {
        case Seq(name, value) => (name, value)
    }.toList.unzip

    lazy val names = nameExprs.map(_.eval(EmptyRow))

    override def nullable: Boolean = true

    override def foldable: Boolean = valExprs.forall(_.foldable)

    override lazy val dataType: StructType = {
        val fields = names.zip(valExprs).map {
            case (name, expr) =>
                val metadata = expr match {
                    case ne: NamedExpression => ne.metadata
                    case _ => Metadata.empty
                }
                StructField(name.toString, expr.dataType, expr.nullable, metadata)
        }
        StructType(fields)
    }

    override def checkInputDataTypes(): TypeCheckResult = {
        if (children.size % 2 != 0) {
            TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
        } else {
            val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)
            if (invalidNames.nonEmpty) {
                TypeCheckResult.TypeCheckFailure(
                    s"Only foldable ${StringType.catalogString} expressions are allowed to appear at odd" +
                        s" position, got: ${invalidNames.mkString(",")}")
            } else if (!names.contains(null)) {
                TypeCheckResult.TypeCheckSuccess
            } else {
                TypeCheckResult.TypeCheckFailure("Field name should not be null")
            }
        }
    }

    /**
     * Returns Aliased [[Expression]]s that could be used to construct a flattened version of this
     * StructType.
     */
    def flatten: Seq[NamedExpression] = valExprs.zip(names).map {
        case (v, n) => Alias(v, n.toString)()
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val rowClass = classOf[GenericInternalRow].getName
        val values = ctx.freshName("values")
        val nonnull = ctx.freshName("nonnull")
        val evals = valExprs.zipWithIndex.map { case (e, i) =>
            val eval = e.genCode(ctx)
            s"""
               |${eval.code}
               |if (${eval.isNull}) {
               |  $values[$i] = null;
               |} else {
               |  $values[$i] = ${eval.value};
               |  $nonnull = true;
               |}
       """.stripMargin
        }
        val codes = ctx.splitExpressionsWithCurrentInputs(
            expressions = evals,
            funcName = "nullable_struct",
            returnType = "boolean",
            extraArguments = "Object[]" -> values :: "boolean" -> nonnull :: Nil,
            makeSplitFunction = body =>
                s"""
                   |do {
                   |  $body
                   |} while (false);
                   |return $nonnull;
                 """.stripMargin,
            foldFunctions = _.map { funcCall =>
                s"""
                   |$nonnull = $funcCall;
                 """.stripMargin
            }.mkString
        )

        import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
        val code =
            code"""
               |Object[] $values = new Object[${valExprs.size}];
               |boolean $nonnull = false;
               |do {
               |  $codes
               |} while (false);
               |
               |boolean ${ev.isNull} = !$nonnull;
               |InternalRow ${ev.value} = null;
               |if (!${ev.isNull}) {
               |  ${ev.value} = new $rowClass($values);
               |}
               |$values = null;
            """.stripMargin
        ev.copy(code = code)
    }

    override def prettyName: String = "named_nullable_struct"

    override def eval(input: InternalRow): Any = {
        val values = valExprs.map(_.eval(input))
        if (values.forall(_ == null))
            null
        else
            InternalRow(values: _*)
    }
}
