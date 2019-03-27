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

package com.dimajix.flowman.templating

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.util.UtcTimestamp

case class FileWrapper(file:File) {
    override def toString: String = file.toString

    def getParent() : FileWrapper = FileWrapper(file.parent)
    def getAbsPath() : FileWrapper = FileWrapper(file.absolute)
    def getFilename() : String = file.filename
    def withSuffix(suffix:String) : FileWrapper = FileWrapper(file.withSuffix(suffix))
    def withName(name:String) : FileWrapper = FileWrapper(file.withName(name))
}

class StringWrapper {
    def concat(c1:String, c2:String) : String = {
        c1 + c2
    }
    def concat(c1:String, c2:String, c3:String) : String = {
        c1 + c2 + c3
    }
    def concat(c1:String, c2:String, c3:String, c4:String) : String = {
        c1 + c2 + c3 + c4
    }
    def concat(c1:String, c2:String, c3:String, c4:String, c5:String) : String = {
        c1 + c2 + c3 + c4 + c5
    }
}

class SystemWrapper {
    def getenv(name:String) : String = {
        Option(System.getenv(name)).getOrElse("")
    }
    def getenv(name:String, default:String) : String = {
        Option(System.getenv(name)).getOrElse(default)
    }
    def getProperty(name:String) : String = {
        System.getProperty(name, "")
    }
    def getProperty(name:String, default:String) : String = {
        System.getProperty(name, default)
    }
}

class TimestampWrapper {
    def parse(value:AnyRef) : UtcTimestamp = UtcTimestamp.parse(value.toString)
    def valueOf(value:AnyRef) : UtcTimestamp = UtcTimestamp.parse(value.toString)
    def toEpochSeconds(value:AnyRef) : Long = UtcTimestamp.toEpochSeconds(value.toString)
    def format(value:AnyRef, format:String) : String = UtcTimestamp.parse(value.toString).format(format)
}

class LocalDateWrapper {
    def parse(value:AnyRef) : LocalDate = LocalDate.parse(value.toString)
    def valueOf(value:AnyRef) : LocalDate = LocalDate.parse(value.toString)
    def format(value:AnyRef, format:String) : String = DateTimeFormatter.ofPattern(format).format(LocalDate.parse(value.toString))
}

class LocalDateTimeWrapper {
    def parse(value:AnyRef) : LocalDateTime = LocalDateTime.parse(value.toString)
    def valueOf(value:AnyRef) : LocalDateTime = LocalDateTime.parse(value.toString)
    def ofEpochSeconds(epoch:AnyRef) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch.toString.toLong, 0, ZoneOffset.UTC)
    def ofEpochSeconds(epoch:Long) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC)
    def format(value:AnyRef, format:String) : String = DateTimeFormatter.ofPattern(format).format(LocalDateTime.parse(value.toString))
}

class DurationWrapper {
    def ofDays(days:AnyRef) : Duration = Duration.ofDays(days.toString.toLong)
    def ofDays(days:Long) : Duration = Duration.ofDays(days)
    def ofHours(hours:AnyRef) : Duration = Duration.ofHours(hours.toString.toLong)
    def ofHours(hours:Long) : Duration = Duration.ofHours(hours)
    def ofMinutes(minutes:AnyRef) : Duration = Duration.ofMinutes(minutes.toString.toLong)
    def ofMinutes(minutes:Long) : Duration = Duration.ofMinutes(minutes)
    def ofSeconds(seconds:AnyRef) : Duration = Duration.ofSeconds(seconds.toString.toLong)
    def ofSeconds(seconds:Long) : Duration = Duration.ofSeconds(seconds)
    def ofMillis(millis:AnyRef) : Duration = Duration.ofMillis(millis.toString.toLong)
    def ofMillis(millis:Long) : Duration = Duration.ofMillis(millis)
    def between(startInclusive: UtcTimestamp, endExclusive: UtcTimestamp) : Duration = Duration.between(startInclusive.toLocalDateTime(), endExclusive.toLocalDateTime())
    def between(startInclusive: Temporal, endExclusive: Temporal) : Duration = Duration.between(startInclusive, endExclusive)
    def between(startInclusive: AnyRef, endExclusive: AnyRef) : Duration = between(UtcTimestamp.parse(startInclusive.toString), UtcTimestamp.parse(endExclusive.toString))
    def parse(value:AnyRef) : Duration = Duration.parse(value.toString)
    def valueOf(value:AnyRef) : Duration = Duration.parse(value.toString)
}

class PeriodWrapper {
    def ofYears(years:AnyRef) : Period = Period.ofYears(years.toString.toInt)
    def ofYears(years:Int) : Period = Period.ofYears(years)
    def ofMonths(months:AnyRef) : Period = Period.ofMonths(months.toString.toInt)
    def ofMonths(months:Int) : Period = Period.ofMonths(months)
    def ofWeeks(weeks:AnyRef) : Period = Period.ofWeeks(weeks.toString.toInt)
    def ofWeeks(weeks:Int) : Period = Period.ofWeeks(weeks)
    def ofDays(days:AnyRef) : Period = Period.ofDays(days.toString.toInt)
    def ofDays(days:Int) : Period = Period.ofDays(days)
    def parse(value:AnyRef) : Period = Period.parse(value.toString)
    def valueOf(value:AnyRef) : Period = Period.parse(value.toString)
}

class BooleanWrapper {
    def parse(value:Boolean) : Boolean = value
    def parse(value:AnyRef) : Boolean = java.lang.Boolean.parseBoolean(value.toString)
    def valueOf(value:AnyRef) : Boolean = java.lang.Boolean.parseBoolean(value.toString)
}

class IntegerWrapper {
    def parse(value:Integer) : Int = value
    def valueOf(value:Integer) : Int = value
    def parse(value:AnyRef) : Int = java.lang.Integer.parseInt(value.toString)
    def valueOf(value:AnyRef) : Int = java.lang.Integer.parseInt(value.toString)
}

class FloatWrapper {
    def parse(value:Integer) : Double = value.toDouble
    def valueOf(value:Integer) : Double = value.toDouble
    def parse(value:Double) : Double = value
    def valueOf(value:Double) : Double = value
    def parse(value:AnyRef) : Double = java.lang.Double.parseDouble(value.toString)
    def valueOf(value:AnyRef) : Double = java.lang.Double.parseDouble(value.toString)
}
