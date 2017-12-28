package com.dimajix.dataflow

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
  * Created by kaya on 11.10.16.
  */
class DateTimeFormatTest extends FlatSpec with Matchers {
    "The instant" should "be formattable in UTC" in {
        val instant = Instant.ofEpochSecond(123456789l).atZone(ZoneId.of("UTC"))
        val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
        formatter.format(instant) should be ("1973/11/29/21")
    }
}
