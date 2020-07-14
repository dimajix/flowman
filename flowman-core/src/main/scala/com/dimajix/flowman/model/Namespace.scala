/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import java.io.File
import java.io.InputStream
import java.net.URL
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.ExternalCatalog
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.metric.ConsoleMetricSink
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.storage.Store


object Namespace {
    private lazy val loader = ServiceLoader.load(classOf[NamespaceReader]).iterator().asScala.toSeq
    private lazy val defaultNamespace = Namespace(
        name = "default",
        metrics = Some(Template.of(new ConsoleMetricSink()))
    )

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Namespace])
        private var format = "yaml"

        def format(fmt:String) : Reader = {
            format = fmt
            this
        }

        def file(file: File): Namespace = {
            logger.info(s"Reading namespace file ${file.toString}")
            reader.file(file)
        }
        def file(filename:String) : Namespace = {
            file(new File(filename))
        }
        def stream(stream:InputStream) : Namespace = {
            reader.stream(stream)
        }
        def url(url:URL) : Namespace = {
            logger.info(s"Reading namespace from url ${url.toString}")
            val stream = url.openStream()
            try {
                reader.stream(stream)
            }
            finally {
                stream.close()
            }
        }
        def string(text:String) : Namespace = {
            reader.string(text)
        }
        def default() : Namespace = defaultNamespace

        private def reader : NamespaceReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new IllegalArgumentException(s"Module format '$format' not supported'"))
        }
    }

    def read = new Reader
}


final case class Namespace(
    name:String,
    config:Map[String,String] = Map(),
    environment:Map[String,String] = Map(),
    profiles:Map[String,Profile] = Map(),
    connections:Map[String,Template[Connection]] = Map(),
    store:Option[Template[Store]] = None,
    catalog:Option[Template[ExternalCatalog]] = None,
    history:Option[Template[StateStore]] = None,
    metrics:Option[Template[MetricSink]] = None,
    plugins:Seq[String] = Seq()
){
}


abstract class NamespaceReader {
    def name : String
    def format : String

    def supports(format:String) : Boolean = this.format == format

    def file(file: File) : Namespace
    def stream(stream: InputStream) : Namespace
    def string(text: String): Namespace
}
