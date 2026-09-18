package org.apache.spark.sql.catalyst.expressions

case class IfNull(left: Expression, right: Expression, replacement: Expression)
    extends RuntimeReplaceable with InheritAnalysisRules {

    def this(left: Expression, right: Expression) = {
        this(left, right, Coalesce(Seq(left, right)))
    }

    override def parameters: Seq[Expression] = Seq(left, right)

    override protected def withNewChildInternal(newChild: Expression): IfNull =
        copy(replacement = newChild)
}
