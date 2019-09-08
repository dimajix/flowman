/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.SchemaWriter


case class SchemaTarget(
    instanceProperties: Target.Properties,
    schema: Schema,
    location:Path,
    format:String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[SchemaTarget])

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides: Seq[ResourceIdentifier] = Seq(
        ResourceIdentifier("schema", location.toString)
    )

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires: Seq[ResourceIdentifier] = Seq()

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      *
      * @param executor
      */
    override def create(executor: Executor): Unit = {}
    override def migrate(executor: Executor): Unit = {}

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor: Executor): Unit = {
        require(executor != null)

        logger.info(s"Writing schema to file '$location'")
        val file = context.fs.file(location)
        new SchemaWriter(schema.fields).format(format).save(file)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor): Unit = {
        require(executor != null)

        val file = executor.fs.file(location)
        if (!file.exists()) {
            logger.error(s"Verification of target '$identifier' failed - local file '$location' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(location)
        if (outputFile.exists()) {
            logger.info(s"Removing schema file '$location'")
            outputFile.delete()
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override def destroy(executor: Executor): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(location)
        if (outputFile.exists()) {
            logger.info(s"Removing schema file '$location'")
            outputFile.delete()
        }
    }
}



class SchemaTargetSpec extends TargetSpec {
    @JsonProperty(value="schema", required=true) private var schema:SchemaSpec = _
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="format", required=false) private var format:String = "avro"

    override def instantiate(context: Context): SchemaTarget = {
        SchemaTarget(
            instanceProperties(context),
            schema.instantiate(context),
            new Path(context.evaluate(location)),
            context.evaluate(format)
        )
    }
}
