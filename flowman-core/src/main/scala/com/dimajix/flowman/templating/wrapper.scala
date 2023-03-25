/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.io.FileInputStream
import java.io.IOException
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.Charset
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import com.dimajix.flowman.fs.{File, FileUtils}
import com.dimajix.flowman.templating.FileWrapper.logger
import com.dimajix.flowman.util.UtcTimestamp


object FileWrapper {
    private val logger = LoggerFactory.getLogger(classOf[FileWrapper])
    def read(str:String) : String = {
        try {
            val input = new FileInputStream(str)
            try {
                IOUtils.toString(input, Charset.forName("UTF-8"))
            }
            finally {
                input.close()
            }
        }
        catch {
            case ex:IOException =>
                logger.warn(s"Cannot read file '$str', return empty string instead: ${ex.getMessage}")
                ""
        }
    }
}
case class FileWrapper(file:File) {
    override def toString: String = file.toString

    def read() : String = {
        try {
            FileUtils.toString(file)
        }
        catch {
            case ex:IOException =>
                logger.warn(s"Cannot read file '${file.toString}', return empty string instead: ${ex.getMessage}")
                ""
        }
    }
    def getParent() : FileWrapper = FileWrapper(file.parent)
    def getAbsPath() : FileWrapper = FileWrapper(file.absolute)
    def getPath() : String = Path.getPathWithoutSchemeAndAuthority(file.path).toString
    def getFilename() : String = file.name
    def withSuffix(suffix:String) : FileWrapper = FileWrapper(file.withSuffix(suffix))
    def withName(name:String) : FileWrapper = FileWrapper(file.withName(name))
}

case class RecursiveValue(engine:VelocityEngine, context:VelocityContext, value:String) {
    override def toString: String = {
        engine.evaluate(context, "RecursiveValue", value)
    }
}


object JsonWrapper {
    private val conf = com.jayway.jsonpath.Configuration
        .builder()
        .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonProvider())
        .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
        .build();

    def path(json:String, path:String) : Any = {
        import com.jayway.jsonpath.JsonPath.using

        using(conf).parse(json).read[AnyRef](path) match {
            case l:java.util.List[_] => l.get(0)
            case s:String => s
            case d:java.lang.Double => d.doubleValue()
            case i:java.lang.Integer => i.intValue()
            case b:java.lang.Boolean => b.booleanValue()
        }
    }
}
