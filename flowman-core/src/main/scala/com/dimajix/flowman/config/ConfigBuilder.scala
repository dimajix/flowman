package com.dimajix.flowman.config

import java.io.File


private object ConfigHelpers {
    private lazy val classLoader = {
        val cl = Thread.currentThread.getContextClassLoader
        if (cl == null)
            classOf[Configuration].getClassLoader
        else
            cl
    }


    def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
        try {
            converter(s.trim)
        } catch {
            case _: NumberFormatException =>
                throw new IllegalArgumentException(s"$key should be $configType, but was $s")
        }
    }

    def toBoolean(s: String, key: String): Boolean = {
        try {
            s.trim.toBoolean
        } catch {
            case _: IllegalArgumentException =>
                throw new IllegalArgumentException(s"$key should be boolean, but was $s")
        }
    }

    def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
        str.split(",").map(_.trim()).filter(_.nonEmpty).map(converter)
    }

    def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
        v.map(stringConverter).mkString(",")
    }

    def stringToClass[T](s: String, key:String, xface:Class[T]) : Class[_ <: T] = {
        try {
            val clazz = Class.forName(s, true, classLoader)
            clazz.asSubclass(xface)
        }
        catch {
            case e: ClassNotFoundException =>
                throw new RuntimeException(e)
        }
    }

    def classToString[T](clazz:Class[T]) : String = {
        clazz.getCanonicalName
    }
}


class TypedConfigBuilder[T](
    val parent: ConfigBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

    import ConfigHelpers._

    def this(parent: ConfigBuilder, converter: String => T) = {
        this(parent, converter, Option(_:T).map(_.toString).orNull)
    }

    /** Apply a transformation to the user-provided values of the config entry. */
    def transform(fn: T => T): TypedConfigBuilder[T] = {
        new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
    }

    /** Checks if the user-provided value for the config matches the validator. */
    def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
        transform { v =>
            if (!validator(v)) throw new IllegalArgumentException(errorMsg)
            v
        }
    }

    /** Check that user-provided values for the config match a pre-defined set. */
    def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
        transform { v =>
            if (!validValues.contains(v)) {
                throw new IllegalArgumentException(
                    s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
            }
            v
        }
    }

    /** Turns the config entry into a sequence of values of the underlying type. */
    def toSequence: TypedConfigBuilder[Seq[T]] = {
        new TypedConfigBuilder(parent, stringToSeq(_, converter), seqToString(_:Seq[T], stringConverter))
    }

    /** Creates a [[ConfigEntry]] that does not have a default value. */
    def createOptional: OptionalConfigEntry[T] = {
        val entry = new OptionalConfigEntry[T](parent.key,
            converter, stringConverter, parent._doc)
        parent._onCreate.foreach(_(entry))
        entry
    }

    /** Creates a [[ConfigEntry]] that has a default value. */
    def createWithDefault(default: T): ConfigEntry[T] = {
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](parent.key,
            transformedDefault, converter, stringConverter, parent._doc)
        parent._onCreate.foreach(_(entry))
        entry
    }
}


case class ConfigBuilder(key: String) {
    private[config] var _doc = ""
    private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None

    import ConfigHelpers._

    def doc(s: String): ConfigBuilder = {
        _doc = s
        this
    }

    /**
     * Registers a callback for when the config entry is finally instantiated. Currently used by
     * SQLConf to keep track of SQL configuration entries.
     */
    def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
        _onCreate = Option(callback)
        this
    }

    def intConf: TypedConfigBuilder[Int] = {
        new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
    }

    def booleanConf: TypedConfigBuilder[Boolean] = {
        new TypedConfigBuilder(this, toBoolean(_, key))
    }

    def stringConf: TypedConfigBuilder[String] = {
        new TypedConfigBuilder(this, v => v)
    }

    def fileConf: TypedConfigBuilder[File] = {
        new TypedConfigBuilder(this, v => new File(v))
    }

    def classConf[T](xface:Class[T]): TypedConfigBuilder[Class[_ <: T]] = {
        new TypedConfigBuilder(this, stringToClass(_, key, xface), classToString(_))
    }
}
