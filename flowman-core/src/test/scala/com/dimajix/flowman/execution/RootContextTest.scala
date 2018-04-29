package com.dimajix.flowman.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class RootContextTest extends FlatSpec with Matchers {
    "The RootContext" should "provide access to some system variables" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${System.getenv('USER')}") should be (System.getenv("USER"))
    }

    it should "provide access to Java Integer class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Integer.parseInt('2')}") should be ("2")
        context.evaluate("${Integer.valueOf('2')}") should be ("2")
    }

    it should "provide access to Java Float class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Float.parseFloat('2')}") should be ("2.0")
        context.evaluate("${Float.valueOf('2')}") should be ("2.0")
    }

    it should "provide access to Java Duration class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Duration.ofDays(2)}") should be ("PT48H")
        context.evaluate("${Duration.parse('P2D').getSeconds()}") should be ("172800")
    }
}
