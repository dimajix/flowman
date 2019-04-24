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
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl=classOf[AppendEntry], visible = false)
    @JsonSubTypes(value = Array(
        new JsonSubTypes.Type(name = "append", value = classOf[AppendEntry]),
        new JsonSubTypes.Type(name = "lift", value = classOf[LiftEntry]),
        new JsonSubTypes.Type(name = "nest", value = classOf[NestEntry]),
        new JsonSubTypes.Type(name = "struct", value = classOf[StructEntry])
    ))
    abstract class Entry {
        def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder
    }


    object AppendEntry {
        def apply(path:String, keep:Seq[String], drop:Seq[String]) : AppendEntry = {
            val result = new AppendEntry
            result._path = path
            result._keep = keep
            result._drop = drop
            result
        }
    }
    class AppendEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var _path:String = _
        @JsonProperty(value = "keep", required = false) private var _keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var _drop:Seq[String] = Seq()

        def path(implicit context: Context) : String = context.evaluate(_path)
        def keep(implicit context: Context) : Seq[String] = _keep.map(context.evaluate)
        def drop(implicit context: Context) : Seq[String] = _drop.map(context.evaluate)

        override def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder = {
            val path = this.path
            val keep = this.keep
            val drop = this.drop

            builder.columns(
                _.path(if (path == null) "" else path)
                 .keep(keep)
                 .drop(drop)
            )
        }
    }

    object LiftEntry {
        def apply(path:String, columns:Seq[String]) : LiftEntry = {
            val result = new LiftEntry
            result._path = path
            result._columns = columns
            result
        }
    }
    class LiftEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var _path:String = _
        @JsonProperty(value = "columns", required = false) private var _columns:Seq[String] = Seq()

        def path(implicit context: Context) : String = context.evaluate(_path)
        def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)

        override def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder = {
            val path = this.path
            val columns = this.columns

            builder.lift(
                _.path(if (path == null) "" else path)
                    .columns(columns)
            )
        }
    }

    object StructEntry {
        def apply(name:String, columns:Seq[Entry]) : StructEntry = {
            val result = new StructEntry
            result._name = name
            result._columns = columns
            result
        }
    }
    class StructEntry extends Entry {
        @JsonProperty(value = "name", required = false) private var _name:String = _
        @JsonProperty(value = "columns", required = false) private var _columns:Seq[Entry] = Seq()

        def name(implicit context: Context) : String = context.evaluate(_name)
        def columns(implicit context: Context) : Seq[Entry] = _columns

        override def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder = {
            val name = this.name
            val columns = this.columns

            builder.assemble(name)(b => columns.foldLeft(b)((builder, entry) => entry.build(builder)))
        }
    }

    object NestEntry {
        def apply(name:String, path:String, keep:Seq[String], drop:Seq[String]) : NestEntry = {
            val result = new NestEntry
            result._name = name
            result._path = path
            result._keep = keep
            result._drop = drop
            result
        }
    }
    class NestEntry extends Entry {
        @JsonProperty(value = "name", required = false) private var _name:String = _
        @JsonProperty(value = "path", required = false) private var _path:String = _
        @JsonProperty(value = "keep", required = false) private var _keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var _drop:Seq[String] = Seq()

        def name(implicit context: Context) : String = context.evaluate(_name)
        def path(implicit context: Context) : String = context.evaluate(_path)
        def keep(implicit context: Context) : Seq[String] = _keep.map(context.evaluate)
        def drop(implicit context: Context) : Seq[String] = _drop.map(context.evaluate)

        override def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder = {
            val path = this.path
            val keep = this.keep
            val drop = this.drop

            builder.nest(name)(
                _.path(if (path == null) "" else path)
                    .keep(keep)
                    .drop(drop)
            )
        }
    }

    object ExplodeEntry {
        def apply(array:String, columns:Seq[Entry]) : ExplodeEntry = {
            val result = new ExplodeEntry
            result._array = array
            result._columns = columns
            result
        }
    }
    class ExplodeEntry extends Entry {
        @JsonProperty(value = "array", required = false) private var _array:String = _
        @JsonProperty(value = "columns", required = false) private var _columns:Seq[Entry] = Seq()

        def array(implicit context: Context) : String = context.evaluate(_array)
        def columns(implicit context: Context) : Seq[Entry] = _columns

        override def build(builder:Assembler.StructBuilder)(implicit context: Context) : Assembler.StructBuilder = {
            val path = this.array
            val columns = this.columns

            ???
        }
    }

    def apply(input:String, columns:Seq[Entry]) : AssembleMapping = {
        val result = new AssembleMapping
        result._input = input
        result._columns = columns
        result
    }
}


class AssembleMapping extends BaseMapping {
    import com.dimajix.flowman.spec.flow.AssembleMapping.Entry

    private val logger = LoggerFactory.getLogger(classOf[AssembleMapping])

    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = false) private var _columns:Seq[Entry] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[Entry] = _columns

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(input != null)

        implicit val context = executor.context
        val df = input(this.input)
        val asm = assembler
        asm.reassemble(df)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)

        implicit val icontext = context
        val schema = input(this.input)
        val asm = assembler
        asm.reassemble(schema)
    }

    private def assembler(implicit context:Context) : Assembler = {
        val builder = columns.foldLeft(Assembler.builder())((builder, entry) => entry.build(builder))
        builder.build()
    }
}
