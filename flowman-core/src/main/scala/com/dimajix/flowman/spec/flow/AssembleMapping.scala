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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.transforms.Assembler
import com.dimajix.flowman.types.StructType


object AssembleMapping {
    abstract class Entry {
        def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder
    }

    case class AppendEntry(path:String, keep:Seq[String], drop:Seq[String]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.columns(
                _.path(path)
                 .keep(keep)
                 .drop(drop)
            )
        }
    }

    case class LiftEntry(path:String, columns:Seq[String]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.lift(
                _.path(path)
                    .columns(columns)
            )
        }
    }

    case class RenameEntry(path:String, columns:Map[String,String]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.rename(
                _.path(path)
                    .columns(columns.toSeq)
            )
        }
    }

    case class StructEntry(name:String, columns:Seq[Entry]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.assemble(name)(b => columns.foldLeft(b)((builder, entry) => entry.build(builder)))
        }
    }

    case class NestEntry(name:String, path:String, keep:Seq[String], drop:Seq[String]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.nest(name)(
                _.path(path)
                    .keep(keep)
                    .drop(drop)
            )
        }
    }

    object ExplodeEntry {
        def apply(path:String) : ExplodeEntry = {
            ExplodeEntry("", path)
        }
    }
    case class ExplodeEntry(name:String, path:String) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            if (name.nonEmpty) {
                builder.explode(name)(
                    _.path(path)
                )
            }
            else {
                builder.explode(
                    _.path(path)
                )
            }
        }
    }

    def apply(input:String, columns:Seq[Entry]) : AssembleMapping = {
        AssembleMapping(Mapping.Properties(), MappingIdentifier(input), columns)
    }
}


case class AssembleMapping(
    instanceProperties : Mapping.Properties,
    input : MappingIdentifier,
    columns: Seq[AssembleMapping.Entry]
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[AssembleMapping])

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(deps != null)

        logger.info(s"Reassembling input mapping '$input'")

        val df = deps(input)
        val asm = assembler
        asm.reassemble(df)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(deps:Map[MappingIdentifier,StructType]) : StructType = {
        require(deps != null)

        val schema = deps(this.input)
        val asm = assembler
        asm.reassemble(schema)
    }

    private def assembler : Assembler = {
        val builder = columns.foldLeft(Assembler.builder())((builder, entry) => entry.build(builder))
        builder.build()
    }
}




object AssembleMappingSpec {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl=classOf[AppendEntry], visible = false)
    @JsonSubTypes(value = Array(
        new JsonSubTypes.Type(name = "append", value = classOf[AppendEntry]),
        new JsonSubTypes.Type(name = "explode", value = classOf[ExplodeEntry]),
        new JsonSubTypes.Type(name = "lift", value = classOf[LiftEntry]),
        new JsonSubTypes.Type(name = "nest", value = classOf[NestEntry]),
        new JsonSubTypes.Type(name = "rename", value = classOf[RenameEntry]),
        new JsonSubTypes.Type(name = "struct", value = classOf[StructEntry])
    ))
    abstract class Entry {
        def instantiate(context: Context) : AssembleMapping.Entry
    }


    class AppendEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "keep", required = false) private var keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var drop:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val path = context.evaluate(this.path)
            val keep = this.keep.map(context.evaluate)
            val drop = this.drop.map(context.evaluate)
            AssembleMapping.AppendEntry(path, keep, drop)
        }
    }

    class LiftEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val path = context.evaluate(this.path)
            val columns = this.columns.map(context.evaluate)
            AssembleMapping.LiftEntry(path, columns)
        }
    }

    class RenameEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val path = context.evaluate(this.path)
            val columns = this.columns.mapValues(context.evaluate)
            AssembleMapping.RenameEntry(path, columns)
        }
    }

    class StructEntry extends Entry {
        @JsonProperty(value = "name", required = true) private var name:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Seq[Entry] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val name = context.evaluate(this.name)
            val columns = this.columns.map(_.instantiate(context))
            AssembleMapping.StructEntry(name, columns)
        }
    }

    class NestEntry extends Entry {
        @JsonProperty(value = "name", required = true) private var name:String = ""
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "keep", required = false) private var keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var drop:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val name = context.evaluate(this.name)
            val path = context.evaluate(this.path)
            val keep = this.keep.map(context.evaluate)
            val drop = this.drop.map(context.evaluate)
            AssembleMapping.NestEntry(name, path, keep, drop)
        }
    }

    class ExplodeEntry extends Entry {
        @JsonProperty(value = "name", required = false) private var name:String = ""
        @JsonProperty(value = "path", required = true) private var path:String = ""

        def instantiate(context: Context) : AssembleMapping.Entry = {
            val name = context.evaluate(this.name)
            val path = context.evaluate(this.path)
            AssembleMapping.ExplodeEntry(name, path)
        }
    }
}


class AssembleMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = false) private var columns: Seq[AssembleMappingSpec.Entry] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): AssembleMapping = {
        AssembleMapping(
            instanceProperties(context),
            MappingIdentifier.parse(context.evaluate(this.input)),
            columns.map(_.instantiate(context))
        )
    }
}
