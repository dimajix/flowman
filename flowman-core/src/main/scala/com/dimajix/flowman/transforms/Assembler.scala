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

package com.dimajix.flowman.transforms

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.transforms.schema.ColumnTree
import com.dimajix.flowman.transforms.schema.LeafNode
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.transforms.schema.SchemaTree
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.types.StructType


object Assembler {
    abstract class Builder {
        def build() : Assembler
    }

    class ColumnBuilder(name:String="") extends Builder {
        private var _path = Path()
        private val _keep = mutable.ListBuffer[Path]()
        private val _drop = mutable.ListBuffer[Path]()

        def path(p:String) : ColumnBuilder = {
            _path = Path(p)
            this
        }
        def keep(c:String) : ColumnBuilder = {
            _keep += Path(c)
            this
        }
        def keep(c:Seq[String]) : ColumnBuilder = {
            _keep ++= c.map(Path(_))
            this
        }
        def drop(c:String) : ColumnBuilder = {
            _drop += Path(c)
            this
        }
        def drop(c:Seq[String]) : ColumnBuilder = {
            _drop ++= c.map(Path(_))
            this
        }

        override def build(): Assembler = {
            if (name.isEmpty)
                new ColumnAssembler(_path, _keep, _drop)
            else
                new NestAssembler(name, _path, _keep, _drop)
        }
    }

    class LiftBuilder extends Builder {
        private var _path = Path()
        private val _columns = mutable.ListBuffer[Path]()

        def path(c:String) : LiftBuilder = {
            _path = Path(c)
            this
        }
        def column(c:String) : LiftBuilder = {
            _columns += Path(c)
            this
        }
        def columns(c:Seq[String]) : LiftBuilder = {
            _columns ++= c.map(Path(_))
            this
        }

        override def build() : Assembler = {
            new LiftAssembler(_path, _columns)
        }
    }

    class StructBuilder(name:String) extends Builder {
        private val _children = mutable.ListBuffer[Builder]()

        def columns(spec:ColumnBuilder => Unit) : StructBuilder = {
            val builder = new ColumnBuilder()
            spec(builder)
            _children += builder
            this
        }
        def nest(name:String)(spec:ColumnBuilder => Unit) : StructBuilder = {
            val builder = new ColumnBuilder(name)
            spec(builder)
            _children += builder
            this
        }
        def lift(spec:LiftBuilder => Unit) : StructBuilder = {
            val builder = new LiftBuilder
            spec(builder)
            _children += builder
            this
        }
        def assemble(name:String)(spec:StructBuilder => Unit) : StructBuilder = {
            val builder = new StructBuilder(name)
            spec(builder)
            _children += builder
            this
        }
        def explode(spec:ExplodeBuilder => Unit) : StructBuilder = {
            val builder = new ExplodeBuilder
            spec(builder)
            _children += builder
            this
        }

        protected def children(): Seq[Assembler] = {
            _children.map(_.build())
        }

        override def build(): Assembler = {
            new StructAssembler(name, children())
        }
    }

    class ExplodeBuilder extends StructBuilder("") {
        private var _path = Path()

        def path(p:String) : ExplodeBuilder = {
            _path = Path(p)
            this
        }

        override def build(): Assembler = {
            new ExplodeAssembler(_path, children())
        }
    }

    def builder() = new StructBuilder("")
}


/**
  * The main class for reassembling DataFrames (and Flowman Schemas)
  */
sealed abstract class Assembler {
    import com.dimajix.flowman.transforms.schema.ColumnTree.implicits._
    import com.dimajix.flowman.transforms.schema.SchemaTree.implicits._

    /**
      * Generic method for reassembling the given Node of a schema tree
      * @param root
      * @param ops
      * @tparam T
      * @return
      */
    def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]]

    /**
      * Reassembles a Spark DataFrame
      * @param df
      * @return
      */
    def reassemble(df:DataFrame) : DataFrame = {
        val tree = ColumnTree.ofSchema(df.schema)
        val newTree = reassemble(tree)
        val columns = newTree.flatMap(_.children.map(_.mkValue()))
        df.select(columns:_*)
    }

    /**
      * Reassembles a Flowman schema (given as a sequence of fields)
      * @param fields
      * @return
      */
    def reassemble(fields:StructType) : StructType = {
        val tree = SchemaTree.ofSchema(fields)
        val newTree = reassemble(tree)
        val columns = newTree.flatMap(_.children.map(_.mkValue()))
        StructType(columns)
    }
}

/**
  * This Assembler will collect a bunch of columns and return them directly as a list
  * @param path
  * @param keep
  * @param drop
  */
class ColumnAssembler private[transforms] (path:Path, keep:Seq[Path], drop:Seq[Path]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val start = root.find(path)
        val node = if(keep.nonEmpty)
                start.map(_.keep(keep).drop(drop))
            else
                start.map(_.drop(drop))
        node.toSeq.flatMap(_.children)
    }
}

/**
  * This assembler will collect a bunch of columns and nest them into a new structure
  * @param name
  * @param path
  * @param keep
  * @param drop
  */
class NestAssembler private[transforms] (name:String, path:Path, keep:Seq[Path], drop:Seq[Path]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val start = root.find(path)
        val node = if(keep.nonEmpty)
                start.map(_.keep(keep).drop(drop))
            else
                start.map(_.drop(drop))
        node.map(_.withName(name)).toSeq
    }
}

/**
  * This assembler will lift nested columns to the top level and returns a list of lifted columns
  * @param path
  * @param columns
  */
class LiftAssembler private[transforms] (path:Path, columns:Seq[Path]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val start = root.find(path)
        columns.flatMap(p => start.flatMap(_.find(p)))
    }
}

/**
  * This Assembler will explode a single column
  * @param array
  * @param columns
  */
class ExplodeAssembler private[transforms] (array:Path, columns:Seq[Assembler]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        root.find(array).map(node => LeafNode(node.name, ops.explode(node.name, node.mkValue()))).toSeq
    }
}

/**
  * This assembler will recursively collect columns and returns them as a new struct
  * @param name
  * @param columns
  */
class StructAssembler private[transforms] (name:String, columns:Seq[Assembler]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val children = columns.flatMap(asm => asm.reassemble(root))
        if (children.nonEmpty)
            Seq(StructNode(name, None, children))
        else
            Seq()
    }
}
