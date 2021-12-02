/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.ServiceLoader
import scala.collection.JavaConverters._


object LogFilter {
    def filters : Seq[LogFilter] = {
        val loader = ServiceLoader.load(classOf[LogFilter])
        loader.iterator().asScala.toSeq
    }

    def filter(filters:Seq[LogFilter], key:String, value:String) : Option[(String,String)] = {
        filters.foldLeft(Option((key, value)))((kv, f) => kv.flatMap(kv => f.filterConfig(kv._1,kv._2)))
    }
}

abstract class LogFilter {
    /**
     * This method gets called for every config key/value. The method can either return a redacted key/value, which
     * then get logged instead of the original key/value. Or the method may return None, which means that no log
     * is to be produced at all.
     * @param key
     * @param value
     * @return
     */
    def filterConfig(key:String, value:String) : Option[(String,String)]
}
