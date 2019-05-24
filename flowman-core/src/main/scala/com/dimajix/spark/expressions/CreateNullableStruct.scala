package com.dimajix.spark.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.CreateNamedStructLike
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamePlaceholder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode


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
case class CreateNullableNamedStruct(children: Seq[Expression]) extends CreateNamedStructLike {

    override def nullable: Boolean = true

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

        val code =
            s"""
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
