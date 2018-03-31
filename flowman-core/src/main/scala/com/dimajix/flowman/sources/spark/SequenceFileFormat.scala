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

package com.dimajix.flowman.sources.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider
import scala.collection.immutable.Map


class SequenceFileFormat extends DataSourceRegister with RelationProvider {
    override def shortName = "sequencefile"

    override def toString = "SequenceFile"

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        val path = parameters.getOrElse("path", throw new IllegalArgumentException("Missing path"))
        new SequenceFileRelation(sqlContext, path)
    }
}
