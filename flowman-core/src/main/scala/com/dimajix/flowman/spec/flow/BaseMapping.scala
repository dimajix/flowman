/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


/**
  * Common base implementation for the MappingType interface
  */
abstract class BaseMapping extends Mapping {
    protected override def instanceProperties : Mapping.Properties

    /**
      * Returns an identifier for this mapping
      * @return
      */
    override def identifier : MappingIdentifier = MappingIdentifier(name, Option(project).map(_.name))

    /**
      * This method should return true, if the resulting dataframe should be broadcast for map-side joins
      * @return
      */
    override def broadcast : Boolean = instanceProperties.broadcast

    /**
      * This method should return true, if the resulting dataframe should be checkpointed
      * @return
      */
    override def checkpoint : Boolean = instanceProperties.checkpoint

    /**
      * Returns the desired storage level. Default should be StorageLevel.NONE
      * @return
      */
    override def cache : StorageLevel = instanceProperties.cache

    /**
      * Lists all outputs of this mapping. Every mapping should have one "main" output
      * @return
      */
    override def outputs : Seq[String] = Seq("main")

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)

        throw new UnsupportedOperationException(s"Schema inference not supported for mapping $name of type $category")
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType], output:String) : StructType = {
        require(input != null)
        require(output != null && output.nonEmpty)

        describe(input)(output)
    }
}
