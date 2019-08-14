/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.config


abstract class ConfigEntry[T] (
    val key: String,
    val valueConverter: String => T,
    val stringConverter: T => String,
    val doc: String) {

    ConfigEntry.registerEntry(this)

    def evaluate(config: String => Option[String]): T

    def defaultValueString: String

    def defaultValue: Option[T] = None

    override def toString: String = {
        s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc)"
    }
}

class ConfigEntryWithDefault[T] (
    key: String,
    _defaultValue: T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String)
    extends ConfigEntry(key, valueConverter, stringConverter, doc) {

    override def defaultValue: Option[T] = Some(_defaultValue)

    override def defaultValueString: String = stringConverter(_defaultValue)

    override def evaluate(config: String => Option[String]): T = {
        config(key).map(valueConverter).getOrElse(_defaultValue)
    }
}

/**
  * A config entry that does not have a default value.
  */
class OptionalConfigEntry[T](
   key: String,
   val rawValueConverter: String => T,
   val rawStringConverter: T => String,
   doc: String)
    extends ConfigEntry[Option[T]](key,
        s => Some(rawValueConverter(s)),
        v => v.map(rawStringConverter).orNull, doc) {

    override def defaultValueString: String = ConfigEntry.UNDEFINED

    override def evaluate(config: String => Option[String]): Option[T] = {
        config(key).map(rawValueConverter)
    }
}

private object ConfigEntry {

    val UNDEFINED = "<undefined>"

    private val knownConfigs = new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

    def registerEntry(entry: ConfigEntry[_]): Unit = {
        val existing = knownConfigs.putIfAbsent(entry.key, entry)
        require(existing == null, s"Config entry ${entry.key} already registered!")
    }

    def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)
}
