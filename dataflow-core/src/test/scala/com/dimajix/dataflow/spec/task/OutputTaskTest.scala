package com.dimajix.dataflow.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.OutputIdentifier

class OutputTaskTest extends FlatSpec with Matchers {
    "The OutputTask" should "support string assignments from code" in {
        val session = Session.builder().build()
        implicit val context = session.context
        val task = new OutputTask
        task.outputs = Seq("lala")
        task.outputs should equal(Seq(OutputIdentifier("lala",None)))
    }
}
