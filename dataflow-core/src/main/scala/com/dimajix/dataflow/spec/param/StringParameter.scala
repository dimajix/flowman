package com.dimajix.dataflow.spec.param

import com.dimajix.dataflow.execution.Context


class StringParameter extends BaseParameter {
    override def values(argument:String)(implicit context:Context) : Iterable[String] = {
        Seq(context.evaluate(argument))
    }
}
