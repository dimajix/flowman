package com.dimajix.flowman.sources.local.csv

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.TimeZone

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.util.DateTimeUtils


class CsvOptions(parameters:Map[String,String]) {
    private val defaultTimeZoneId = "UTC"

    private def getChar(paramName: String, default: Char): Char = {
        val paramValue = parameters.get(paramName)
        paramValue match {
            case None => default
            case Some(null) => default
            case Some(value) if value.length == 0 => '\u0000'
            case Some(value) if value.length == 1 => value.charAt(0)
            case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
        }
    }
    private def getBool(paramName: String, default: Boolean = false): Boolean = {
        val param = parameters.getOrElse(paramName, default.toString)
        if (param == null) {
            default
        } else if (param.toLowerCase(Locale.ROOT) == "true") {
            true
        } else if (param.toLowerCase(Locale.ROOT) == "false") {
            false
        } else {
            throw new Exception(s"$paramName flag can be true or false")
        }
    }

    val delimiter = CsvUtils.toChar(
        parameters.getOrElse("sep", parameters.getOrElse("delimiter", ",")))
    val encoding = parameters.getOrElse("encoding",
        parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))
    val newline = parameters.getOrElse("newline", "\n")
    val quote = getChar("quote", '\"')
    val escape = getChar("escape", '\\')
    val comment = parameters.getOrElse("comment", "\u0000")

    val quoteAll = getBool("quoteAll", false)
    val escapeQuotes = getBool("escapeQuotes", true)
    val ignoreLeadingWhiteSpaceFlag = getBool("ignoreLeadingWhiteSpace", true)
    val ignoreTrailingWhiteSpaceFlag = getBool("ignoreTrailingWhiteSpace", true)

    val headerFlag = getBool("header", false)
    val inferSchemaFlag = getBool("inferSchema", false)

    val nullValue = parameters.getOrElse("nullValue", "")
    val nanValue = parameters.getOrElse("nanValue", "NaN")
    val positiveInf = parameters.getOrElse("positiveInf", "Inf")
    val negativeInf = parameters.getOrElse("negativeInf", "-Inf")

    val timeZone: TimeZone = DateTimeUtils.getTimeZone(
        parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

    // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
    val dateFormat: FastDateFormat =
        FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

    val timestampFormat: FastDateFormat =
        FastDateFormat.getInstance(
            parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)
}
