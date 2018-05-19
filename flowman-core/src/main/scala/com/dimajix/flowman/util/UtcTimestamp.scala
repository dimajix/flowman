/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.util

import java.sql.Timestamp
import java.time.DayOfWeek
import java.time.Duration
import java.time.LocalDateTime
import java.time.Month
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


object UtcTimestamp {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss][.S]").withZone(ZoneOffset.UTC)

    def now() : UtcTimestamp = {
        val ld = LocalDateTime.now(ZoneOffset.UTC)
        new UtcTimestamp(ld)
    }

    def of(year: Int, month: Month, dayOfMonth: Int, hour: Int, minute: Int) : UtcTimestamp = {
        val ld = LocalDateTime.of(year, month, dayOfMonth, hour, minute)
        new UtcTimestamp(ld)
    }

    /**
      * Parses a string as a UTC timestamp and returns a UtcTimestamp object
      * @param value
      * @return
      */
    def parse(value:String) : UtcTimestamp = {
        new UtcTimestamp(LocalDateTime.parse(value, formatter))
    }

    /**
      * Parses a string as a UTC timestamp and returns the epoch in seconds
      * @param value
      * @return
      */
    def toEpochSeconds(value:String) : Long = {
        LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC)
    }
}


/**
  * This is a helper class which uses UTC in its "toString" method as opposed to the java.sql.Timestamp class
  * which uses local date time
  * @param dt
  */
class UtcTimestamp(dt:LocalDateTime) {
    import UtcTimestamp.formatter

    def this(msecs:Long) = {
        this(LocalDateTime.ofEpochSecond(msecs / 1000, 0, ZoneOffset.UTC))
    }

    override def toString: String = {
        dt.format(formatter)
    }

    def toEpochSeconds() : Long = {
        dt.toEpochSecond(ZoneOffset.UTC)
    }

    def toLocalDateTime() : LocalDateTime = {
        dt
    }

    def toTimestamp() : Timestamp = {
        new Timestamp(toEpochSeconds * 1000l)
    }

    def getYear() : Int = dt.getYear
    def getMonth() : Int = dt.getMonthValue
    def getDayOfMonth() : Int = dt.getDayOfMonth
    def getDayOfYear() : Int = dt.getDayOfYear
    def getDayOfWeek() : DayOfWeek = dt.getDayOfWeek
    def getHour() : Int = dt.getHour
    def getMinute() : Int = dt.getMinute
    def getSecond() : Int = dt.getSecond

    def plus(duration:Duration) : UtcTimestamp = new UtcTimestamp(dt.plus(duration))
    def minus(duration:Duration) : UtcTimestamp = new UtcTimestamp(dt.minus(duration))

    def plusYears(years:Int) : UtcTimestamp = new UtcTimestamp(dt.plusYears(years))
    def plusMonths(months:Int) : UtcTimestamp = new UtcTimestamp(dt.plusMonths(months))
    def plusWeeks(weeks:Int) : UtcTimestamp = new UtcTimestamp(dt.plusWeeks(weeks))
    def plusDays(days:Int) : UtcTimestamp = new UtcTimestamp(dt.plusDays(days))
    def plusHours(hours:Int) : UtcTimestamp = new UtcTimestamp(dt.plusHours(hours))
    def plusMinutes(minutes:Int) : UtcTimestamp = new UtcTimestamp(dt.plusMinutes(minutes))
    def plusSeconds(seconds:Int) : UtcTimestamp = new UtcTimestamp(dt.plusSeconds(seconds))

    def minusYears(years:Int) : UtcTimestamp = new UtcTimestamp(dt.minusYears(years))
    def minusMonths(months:Int) : UtcTimestamp = new UtcTimestamp(dt.minusMonths(months))
    def minusWeeks(weeks:Int) : UtcTimestamp = new UtcTimestamp(dt.minusWeeks(weeks))
    def minusDays(days:Int) : UtcTimestamp = new UtcTimestamp(dt.minusDays(days))
    def minusHours(hours:Int) : UtcTimestamp = new UtcTimestamp(dt.minusHours(hours))
    def minusMinutes(minutes:Int) : UtcTimestamp = new UtcTimestamp(dt.minusMinutes(minutes))
    def minusSeconds(seconds:Int) : UtcTimestamp = new UtcTimestamp(dt.minusSeconds(seconds))

    def format(fmt:String) : String =  DateTimeFormatter.ofPattern(fmt).format(dt)
}
