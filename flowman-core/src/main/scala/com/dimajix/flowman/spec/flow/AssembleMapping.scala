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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
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

    case class FlattenEntry(path:String, keep:Seq[String], drop:Seq[String], prefix:String="", naming:String="snakeCase") extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.flatten(
                _.path(path)
                    .keep(keep)
                    .drop(drop)
                    .prefix(prefix)
                    .naming(naming)
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
}


case class AssembleMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    columns: Seq[AssembleMapping.Entry]
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(executor != null)
        require(deps != null)

        val df = deps(input)
        val asm = assembler
        val result = asm.reassemble(df)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(executor:Executor, deps:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(deps != null)

        val schema = deps(this.input)
        val asm = assembler
        val result = asm.reassemble(schema)

        Map("main" -> result)
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
        new JsonSubTypes.Type(name = "flatten", value = classOf[FlattenEntry]),
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
            AssembleMapping.AppendEntry(
                context.evaluate(path),
                keep.map(context.evaluate),
                drop.map(context.evaluate)
            )
        }
    }

    class FlattenEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "keep", required = false) private var keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var drop:Seq[String] = Seq()
        @JsonProperty(value = "prefix", required = false) private var prefix:String = ""
        @JsonProperty(value = "naming", required = false) private var naming:String = "snakeCase"

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.FlattenEntry(
                context.evaluate(path),
                keep.map(context.evaluate),
                drop.map(context.evaluate),
                prefix,
                naming
            )
        }
    }

    class LiftEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.LiftEntry(
                context.evaluate(path),
                columns.map(context.evaluate)
            )
        }
    }

    class RenameEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.RenameEntry(
                context.evaluate(path),
                context.evaluate(columns)
            )
        }
    }

    class StructEntry extends Entry {
        @JsonProperty(value = "name", required = true) private var name:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Seq[Entry] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.StructEntry(
                context.evaluate(name),
                columns.map(_.instantiate(context))
            )
        }
    }

    class NestEntry extends Entry {
        @JsonProperty(value = "name", required = true) private var name:String = ""
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "keep", required = false) private var keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var drop:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.NestEntry(
                context.evaluate(name),
                context.evaluate(path),
                keep.map(context.evaluate),
                drop.map(context.evaluate)
            )
        }
    }

    class ExplodeEntry extends Entry {
        @JsonProperty(value = "name", required = false) private var name:String = ""
        @JsonProperty(value = "path", required = true) private var path:String = ""

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.ExplodeEntry(
                context.evaluate(name),
                context.evaluate(path)
            )
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
            MappingOutputIdentifier.parse(context.evaluate(this.input)),
            columns.map(_.instantiate(context))
        )
    }
}
