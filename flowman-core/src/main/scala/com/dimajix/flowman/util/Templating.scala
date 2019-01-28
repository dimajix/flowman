package com.dimajix.flowman.util

import java.time.Duration
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneOffset
import java.time.temporal.Temporal

import scala.collection.mutable

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine

import com.dimajix.flowman.fs.File


object Templating {
    private val classes = mutable.Map[String,Class[_]]()

    case class FileWrapper(file:File) {
        override def toString: String = file.toString

        def getParent() : FileWrapper = FileWrapper(file.parent)
        def getAbsPath() : FileWrapper = FileWrapper(file.absolute)
        def getFilename() : String = file.filename
        def withSuffix(suffix:String) : FileWrapper = FileWrapper(file.withSuffix(suffix))
        def withName(name:String) : FileWrapper = FileWrapper(file.withName(name))
    }

    private object StringWrapper {
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

    private object SystemWrapper {
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

    private object TimestampWrapper {
        def parse(value:String) : UtcTimestamp = UtcTimestamp.parse(value)
        def valueOf(value:String) : UtcTimestamp = UtcTimestamp.parse(value)
        def toEpochSeconds(value:String) : Long = UtcTimestamp.toEpochSeconds(value)
    }

    private object LocalDateTimeWrapper {
        def parse(value:String) : LocalDateTime = LocalDateTime.parse(value)
        def valueOf(value:String) : LocalDateTime = LocalDateTime.parse(value)
        def ofEpochSeconds(epoch:String) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch.toLong, 0, ZoneOffset.UTC)
        def ofEpochSeconds(epoch:Long) : LocalDateTime = LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC)
    }

    private object DurationWrapper {
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

    private object PeriodWrapper {
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

    private object BooleanWrapper {
        def parse(value:Boolean) : Boolean = value
        def parse(value:String) : Boolean = java.lang.Boolean.parseBoolean(value)
        def valueOf(value:String) : Boolean = java.lang.Boolean.parseBoolean(value)
    }

    private object IntegerWrapper {
        def parse(value:Integer) : Int = value
        def valueOf(value:Integer) : Int = value
        def parse(value:String) : Int = java.lang.Integer.parseInt(value)
        def valueOf(value:String) : Int = java.lang.Integer.parseInt(value)
    }

    private object FloatWrapper {
        def parse(value:Integer) : Double = value.toDouble
        def valueOf(value:Integer) : Double = value.toDouble
        def parse(value:Double) : Double = value
        def valueOf(value:Double) : Double = value
        def parse(value:String) : Double = java.lang.Double.parseDouble(value)
        def valueOf(value:String) : Double = java.lang.Double.parseDouble(value)
    }

    def addClass(name:String, aClass:Class[_]) : Unit = {
        classes.update(name, aClass)
    }


    /**
      * Creates a new VelocityContext with all templating objects preregistered in the context
      * @return
      */
    def newContext() : VelocityContext = {
        val context = new VelocityContext()
        context.put("Boolean", BooleanWrapper)
        context.put("Integer", IntegerWrapper)
        context.put("Float", FloatWrapper)
        context.put("LocalDateTime", LocalDateTimeWrapper)
        context.put("Timestamp", TimestampWrapper)
        context.put("Duration", DurationWrapper)
        context.put("Period", PeriodWrapper)
        context.put("System", SystemWrapper)
        context.put("String", StringWrapper)

        // Add instances of all custom classses
        classes.foreach { case (name, cls) => context.put(name, cls.newInstance()) }

        context
    }

    /**
      * Creates a new VelocityEngine
      * @return
      */
    def newEngine() : VelocityEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }
}
