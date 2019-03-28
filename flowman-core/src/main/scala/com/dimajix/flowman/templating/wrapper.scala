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
    def parse(value:String) : UtcTimestamp = UtcTimestamp.parse(value)
    def valueOf(value:String) : UtcTimestamp = UtcTimestamp.parse(value)
    def toEpochSeconds(value:String) : Long = UtcTimestamp.toEpochSeconds(value)
    def format(value:String, format:String) : String = UtcTimestamp.parse(value).format(format)
}

class LocalDateWrapper {
    def parse(value:String) : LocalDate = LocalDate.parse(value)
    def valueOf(value:String) : LocalDate = LocalDate.parse(value)
    def format(value:String, format:String) : String = DateTimeFormatter.ofPattern(format).format(LocalDate.parse(value))
}

class LocalDateTimeWrapper {
    def parse(value:String) : LocalDateTime = LocalDateTime.parse(value)
    def valueOf(value:String) : LocalDateTime = LocalDateTime.parse(value)
    def ofEpochSeconds(epoch:String) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch.toLong, 0, ZoneOffset.UTC)
    def ofEpochSeconds(epoch:Long) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC)
    def format(value:String, format:String) : String = DateTimeFormatter.ofPattern(format).format(LocalDateTime.parse(value))
}

class DurationWrapper {
    def ofDays(days:String) : Duration = Duration.ofDays(days.toLong)
    def ofDays(days:Long) : Duration = Duration.ofDays(days)
    def ofHours(hours:String) : Duration = Duration.ofHours(hours.toLong)
    def ofHours(hours:Long) : Duration = Duration.ofHours(hours)
    def ofMinutes(minutes:String) : Duration = Duration.ofMinutes(minutes.toLong)
    def ofMinutes(minutes:Long) : Duration = Duration.ofMinutes(minutes)
    def ofSeconds(seconds:String) : Duration = Duration.ofSeconds(seconds.toLong)
    def ofSeconds(seconds:Long) : Duration = Duration.ofSeconds(seconds)
    def ofMillis(millis:String) : Duration = Duration.ofMillis(millis.toLong)
    def ofMillis(millis:Long) : Duration = Duration.ofMillis(millis)
    def between(startInclusive: UtcTimestamp, endExclusive: UtcTimestamp) : Duration = Duration.between(startInclusive.toLocalDateTime(), endExclusive.toLocalDateTime())
    def between(startInclusive: Temporal, endExclusive: Temporal) : Duration = Duration.between(startInclusive, endExclusive)
    def between(startInclusive: String, endExclusive: String) : Duration = between(UtcTimestamp.parse(startInclusive), UtcTimestamp.parse(endExclusive))
    def parse(value:String) : Duration = Duration.parse(value)
    def valueOf(value:String) : Duration = Duration.parse(value)
}

class PeriodWrapper {
    def ofYears(years:String) : Period = Period.ofYears(years.toInt)
    def ofYears(years:Int) : Period = Period.ofYears(years)
    def ofMonths(months:String) : Period = Period.ofMonths(months.toInt)
    def ofMonths(months:Int) : Period = Period.ofMonths(months)
    def ofWeeks(weeks:String) : Period = Period.ofWeeks(weeks.toInt)
    def ofWeeks(weeks:Int) : Period = Period.ofWeeks(weeks)
    def ofDays(days:String) : Period = Period.ofDays(days.toInt)
    def ofDays(days:Int) : Period = Period.ofDays(days)
    def parse(value:String) : Period = Period.parse(value)
    def valueOf(value:String) : Period = Period.parse(value)
}

class BooleanWrapper {
    def parse(value:Boolean) : Boolean = value
    def parse(value:String) : Boolean = java.lang.Boolean.parseBoolean(value)
    def valueOf(value:String) : Boolean = java.lang.Boolean.parseBoolean(value)
}

class IntegerWrapper {
    def parse(value:Integer) : Int = value
    def valueOf(value:Integer) : Int = value
    def parse(value:String) : Int = java.lang.Integer.parseInt(value)
    def valueOf(value:String) : Int = java.lang.Integer.parseInt(value)
}

class FloatWrapper {
    def parse(value:Integer) : Double = value.toDouble
    def valueOf(value:Integer) : Double = value.toDouble
    def parse(value:Double) : Double = value
    def valueOf(value:Double) : Double = value
    def parse(value:String) : Double = java.lang.Double.parseDouble(value)
    def valueOf(value:String) : Double = java.lang.Double.parseDouble(value)
}
