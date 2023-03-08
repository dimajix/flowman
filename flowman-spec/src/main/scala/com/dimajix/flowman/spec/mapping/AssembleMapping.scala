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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.transforms.Assembler
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.ExpressionParser


object AssembleMapping {
    sealed abstract class Entry {
        def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder
    }

    case class AppendEntry(path:Path=Path.empty, keep:Seq[Path]=Seq(), drop:Seq[Path]=Seq()) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.columns(
                _.path(path)
                 .keep(keep)
                 .drop(drop)
            )
        }
    }

    case class FlattenEntry(path:Path=Path.empty, keep:Seq[Path]=Seq(), drop:Seq[Path]=Seq(), prefix:String="", naming:CaseFormat=CaseFormat.SNAKE_CASE) extends Entry {
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

    case class LiftEntry(path:Path=Path.empty, columns:Seq[Path]) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.lift(
                _.path(path)
                    .columns(columns)
            )
        }
    }

    case class RenameEntry(path:Path=Path.empty, columns:Map[String,Path]) extends Entry {
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

    case class NestEntry(name:String, path:Path=Path.empty, keep:Seq[Path]=Seq(), drop:Seq[Path]=Seq()) extends Entry {
        override def build(builder:Assembler.StructBuilder) : Assembler.StructBuilder = {
            builder.nest(name)(
                _.path(path)
                    .keep(keep)
                    .drop(drop)
            )
        }
    }

    object ExplodeEntry {
        def apply(path:Path) : ExplodeEntry = {
            ExplodeEntry("", path)
        }
    }
    case class ExplodeEntry(name:String, path:Path) extends Entry {
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
    columns: Seq[AssembleMapping.Entry],
    filter:Option[String] = None
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param deps
      * @return
      */
    override def execute(execution: Execution, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(execution != null)
        require(deps != null)

        val df = deps(input)
        val asm = assembler
        val result = asm.reassemble(df)

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, deps)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(execution:Execution, deps:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(deps != null)

        val schema = deps(this.input)
        val asm = assembler
        val result = asm.reassemble(schema)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
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
                Path(context.evaluate(path)),
                keep.map(p => Path(context.evaluate(p))),
                drop.map(p => Path(context.evaluate(p)))
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
                Path(context.evaluate(path)),
                keep.map(p => Path(context.evaluate(p))),
                drop.map(p => Path(context.evaluate(p))),
                prefix,
                CaseFormat.ofString(context.evaluate(naming))
            )
        }
    }

    class LiftEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Seq[String] = Seq()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.LiftEntry(
                Path(context.evaluate(path)),
                columns.map(p => Path(context.evaluate(p)))
            )
        }
    }

    class RenameEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var path:String = ""
        @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.RenameEntry(
                Path(context.evaluate(path)),
                context.evaluate(columns).map(kv => kv._1 -> Path(kv._2))
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
                Path(context.evaluate(path)),
                keep.map(p => Path(context.evaluate(p))),
                drop.map(p => Path(context.evaluate(p)))
            )
        }
    }

    class ExplodeEntry extends Entry {
        @JsonProperty(value = "name", required = false) private var name:String = ""
        @JsonProperty(value = "path", required = true) private var path:String = ""

        def instantiate(context: Context) : AssembleMapping.Entry = {
            AssembleMapping.ExplodeEntry(
                context.evaluate(name),
                Path(context.evaluate(path))
            )
        }
    }
}


class AssembleMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = false) private var columns: Seq[AssembleMappingSpec.Entry] = Seq()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): AssembleMapping = {
        AssembleMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(this.input)),
            columns.map(_.instantiate(context)),
            context.evaluate(filter)
        )
    }
}
