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

package com.dimajix.flowman.spec.mapping

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{types => st}

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.transforms.ExplodeTransformer
import com.dimajix.flowman.transforms.FlattenTransformer
import com.dimajix.flowman.transforms.IdentityTransformer
import com.dimajix.flowman.transforms.LiftTransformer
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.StructType


object ExplodeMapping {
    case class Columns(
        keep: Seq[Path] = Seq(),
        drop: Seq[Path] = Seq(),
        rename: Map[String,Path] = Map()
    )
}

case class ExplodeMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    array: Path,
    outerColumns: ExplodeMapping.Columns = ExplodeMapping.Columns(),
    innerColumns: ExplodeMapping.Columns = ExplodeMapping.Columns(),
    flatten: Boolean = false,
    naming: CaseFormat = CaseFormat.SNAKE_CASE
) extends BaseMapping {
    override def outputs: Seq[String] = Seq("main", "explode")

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] =  {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param deps
      * @return
      */
    override def execute(execution: Execution, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        require(execution != null)
        require(deps != null)

        def isSimpleArray(df:DataFrame) : Boolean = {
            val arrayTail = array.last.toString.toLowerCase(Locale.ROOT)
            val field = df.schema.fields.find(_.name.toLowerCase(Locale.ROOT) == arrayTail).get
            field.dataType match {
                case _:st.StructType => false
                case _ => true
            }
        }

        val in = deps(input)
        val exploded = explode.transform(in)
        val lifted =
            if (isSimpleArray(exploded))
                exploded
            else
                lift.transform(exploded)
        val result = flat.transform(lifted)

        Map("main" -> result, "explode" -> exploded)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(execution:Execution, deps:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(deps != null)

        def isSimpleArray(dt:StructType) : Boolean = {
            val arrayTail = array.last.toString.toLowerCase(Locale.ROOT)
            val field = dt.fields.find(_.name.toLowerCase(Locale.ROOT) == arrayTail).get
            field.ftype match {
                case _:StructType => false
                case _ => true
            }
        }

        val in = deps(input)
        val exploded = explode.transform(in)
        val lifted =
            if (isSimpleArray(exploded))
                exploded
            else
                lift.transform(exploded)
        val result = flat.transform(lifted)

        Map("main" -> result, "explode" -> exploded)
    }

    private def explode = ExplodeTransformer(array, outerColumns.keep, outerColumns.drop, outerColumns.rename)
    private def lift = LiftTransformer(array.last, innerColumns.keep, innerColumns.drop, innerColumns.rename)
    private def flat = if (flatten) FlattenTransformer(naming) else IdentityTransformer()
}



object ExplodeMappingSpec {
    class Columns {
        @JsonProperty(value = "keep", required = true) private var keep: Seq[String] = Seq()
        @JsonProperty(value = "drop", required = true) private var drop: Seq[String] = Seq()
        @JsonProperty(value = "rename", required = true) private var rename: Map[String,String] = Map()

        def instantiate(context: Context) : ExplodeMapping.Columns = {
            ExplodeMapping.Columns(
                keep.map(p => Path(context.evaluate(p))),
                drop.map(p => Path(context.evaluate(p))),
                rename.map { case (k,v) => (k, Path(context.evaluate(v))) }
            )
        }
    }
}
class ExplodeMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "array", required = true) private var array: String = _
    @JsonProperty(value = "flatten", required = false) private var flatten: String = "false"
    @JsonProperty(value = "naming", required = false) private var naming: String = "snakeCase"
    @JsonProperty(value = "outerColumns", required = false) private var outerColumns: ExplodeMappingSpec.Columns = new ExplodeMappingSpec.Columns()
    @JsonProperty(value = "innerColumns", required = false) private var innerColumns: ExplodeMappingSpec.Columns = new ExplodeMappingSpec.Columns()

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context): ExplodeMapping = {
        ExplodeMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            Path(context.evaluate(array)),
            outerColumns.instantiate(context),
            innerColumns.instantiate(context),
            context.evaluate(flatten).toBoolean,
            CaseFormat.ofString(context.evaluate(naming))
        )
    }
}
