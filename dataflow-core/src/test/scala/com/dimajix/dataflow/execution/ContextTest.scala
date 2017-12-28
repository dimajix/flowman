package com.dimajix.dataflow.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.dataflow.LocalSparkSession


class ContextTest extends FlatSpec with Matchers with LocalSparkSession {
    "Evaluation" should "work (1)" in {
        val context = new Context(session)
        context.setEnvironment("env_1", "value_1")
        context.evaluate("$env_1") should be ("value_1")
    }

    "Evaluation" should "work (2)" in {
        val context = new Context(session)
        context.setEnvironment("env_1", "value_1")
        context.setEnvironment("env_2", "$env_1")
        context.setEnvironment("env_3", "$env_2")
        context.evaluate("$env_2") should be ("value_1")
        context.evaluate("$env_3") should be ("value_1")
    }
}
