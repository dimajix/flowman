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

package com.dimajix.flowman.transforms

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.transforms.schema.ArrayNode
import com.dimajix.flowman.transforms.schema.ColumnTree
import com.dimajix.flowman.transforms.schema.LeafNode
import com.dimajix.flowman.transforms.schema.MapNode
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

    abstract class RecursiveBuilder extends Builder {
        private val _children = mutable.ListBuffer[Builder]()

        protected def addChild(child:Builder) : Unit = {
            _children += child
        }
        protected def children(): Seq[Assembler] = {
            _children.map(_.build())
        }
    }

    /**
      * This builder is used to either keep certain columns or to drop columns
      * @param name
      */
    class ColumnBuilder(name:String="") extends Builder {
        private var _path = Path()
        private val _keep = mutable.ListBuffer[Path]()
        private val _drop = mutable.ListBuffer[Path]()

        def path(p:Path) : ColumnBuilder = {
            _path = p
            this
        }
        def keep(c:Path) : ColumnBuilder = {
            _keep += c
            this
        }
        def keep(c:Seq[Path]) : ColumnBuilder = {
            _keep ++= c
            this
        }
        def drop(c:Path) : ColumnBuilder = {
            _drop += c
            this
        }
        def drop(c:Seq[Path]) : ColumnBuilder = {
            _drop ++= c
            this
        }

        override def build(): Assembler = {
            if (name.isEmpty)
                new ColumnAssembler(_path, _keep, _drop)
            else
                new NestAssembler(name, _path, _keep, _drop)
        }
    }

    class FlattenBuilder extends Builder {
        private var _path = Path()
        private val _keep = mutable.ListBuffer[Path]()
        private val _drop = mutable.ListBuffer[Path]()
        private var _prefix = ""
        private var _naming:CaseFormat = CaseFormat.SNAKE_CASE

        def path(p:Path) : FlattenBuilder = {
            _path = p
            this
        }
        def keep(c:Path) : FlattenBuilder = {
            _keep += c
            this
        }
        def keep(c:Seq[Path]) : FlattenBuilder = {
            _keep ++= c
            this
        }
        def drop(c:Path) : FlattenBuilder = {
            _drop += c
            this
        }
        def drop(c:Seq[Path]) : FlattenBuilder = {
            _drop ++= c
            this
        }
        def prefix(p:String) : FlattenBuilder = {
            _prefix = p
            this
        }
        def naming(n:CaseFormat) : FlattenBuilder = {
            _naming = n
            this
        }

        override def build(): Assembler = {
            new FlattenAssembler(_path, _keep, _drop, _prefix, _naming)
        }
    }

    /**
      * This builder is used to lift nested columns to the top level
      */
    class LiftBuilder extends Builder {
        private var _path = Path()
        private val _columns = mutable.ListBuffer[Path]()

        def path(c:Path) : LiftBuilder = {
            _path = c
            this
        }
        def column(c:Path) : LiftBuilder = {
            _columns += c
            this
        }
        def columns(c:Seq[Path]) : LiftBuilder = {
            _columns ++= c
            this
        }

        override def build() : Assembler = {
            new LiftAssembler(_path, _columns)
        }
    }

    /**
      * This builder is used to assemble the output of multiple sub-builders
      * @param name
      */
    class StructBuilder(name:String) extends RecursiveBuilder {
        def columns(spec:ColumnBuilder => Unit) : StructBuilder = {
            val builder = new ColumnBuilder()
            spec(builder)
            addChild(builder)
            this
        }
        def flatten(spec:FlattenBuilder => Unit) : StructBuilder = {
            val builder = new FlattenBuilder()
            spec(builder)
            addChild(builder)
            this
        }
        def nest(name:String)(spec:ColumnBuilder => Unit) : StructBuilder = {
            val builder = new ColumnBuilder(name)
            spec(builder)
            addChild(builder)
            this
        }
        def lift(spec:LiftBuilder => Unit) : StructBuilder = {
            val builder = new LiftBuilder
            spec(builder)
            addChild(builder)
            this
        }
        def assemble(name:String)(spec:StructBuilder => Unit) : StructBuilder = {
            val builder = new StructBuilder(name)
            spec(builder)
            addChild(builder)
            this
        }
        def rename(spec:RenameBuilder => Unit) : StructBuilder = {
            val builder = new RenameBuilder
            spec(builder)
            addChild(builder)
            this
        }
        def explode(name:String)(spec:ExplodeBuilder => Unit) : StructBuilder = {
            val builder = new ExplodeBuilder(name)
            spec(builder)
            addChild(builder)
            this
        }
        def explode(spec:ExplodeBuilder => Unit) : StructBuilder = {
            val builder = new ExplodeBuilder("")
            spec(builder)
            addChild(builder)
            this
        }

        override def build(): Assembler = {
            new StructAssembler(name, children())
        }
    }

    /**
      * This builder is simply used for renaming individual columns
      */
    class RenameBuilder extends Builder {
        private var _path = Path()
        private val _columns = mutable.ListBuffer[(String,Path)]()

        def path(p:Path) : RenameBuilder = {
            _path = p
            this
        }
        def column(newName:String, oldName:Path) : RenameBuilder = {
            _columns += ((newName, oldName))
            this
        }
        def columns(c:Seq[(String,Path)]) : RenameBuilder = {
            _columns ++= c
            this
        }

        override def build(): Assembler = {
            new RenameAssembler(_path, _columns)
        }
    }

    class ExplodeBuilder(name:String) extends Builder {
        private var _path = Path()

        def path(p:Path) : ExplodeBuilder = {
            _path = p
            this
        }

        override def build(): Assembler = {
            new ExplodeAssembler(name, _path)
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
      * @param schema
      * @return
      */
    def reassemble(schema:StructType) : StructType = {
        val tree = SchemaTree.ofSchema(schema)
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
  * This Assembler will collect a bunch of columns and return them directly as a list
  * @param path
  * @param keep
  * @param drop
  */
class FlattenAssembler private[transforms] (path:Path,keep:Seq[Path], drop:Seq[Path], prefix:String="", naming:CaseFormat=CaseFormat.SNAKE_CASE) extends Assembler {
    private def rename(prefix:String, name:String) : String = {
        naming.concat(prefix, name)
    }

    private def flatten[T](node:Node[T], prefix:String) : Seq[Node[T]] = {
        node match {
            case leaf:LeafNode[T] => Seq(leaf.withName(rename(prefix, leaf.name)))
            case struct:StructNode[T] => struct.children.flatMap(flatten(_, rename(prefix, struct.name)))
            case array:ArrayNode[T] => Seq(array.withName(rename(prefix , array.name)))
            case map:MapNode[T] => Seq(map.withName(rename(prefix, map.name)))
        }
    }

    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val start = root.find(path)
        val node = if(keep.nonEmpty)
            start.map(_.keep(keep).drop(drop))
        else
            start.map(_.drop(drop))
        node.toSeq.flatMap(_.children.flatMap(flatten(_, prefix)))
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
        val node = root
            .find(path)
            .getOrElse(throw new IllegalArgumentException(s"Path $path not found in rename"))

        val struct = if(keep.nonEmpty)
                node.keep(keep).drop(drop)
            else
                node.drop(drop)
        Seq(struct.withName(name))
    }
}

/**
  * This assembler will lift nested columns to the top level and returns a list of lifted columns
  * @param path
  * @param columns
  */
class LiftAssembler private[transforms] (path:Path, columns:Seq[Path]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val node = root
            .find(path)
            .getOrElse(throw new IllegalArgumentException(s"Path $path not found in rename"))

        columns.flatMap(p => node.find(p))
    }
}

/**
  * This assembler will lift nested columns to the top level and returns a list of lifted columns
  * @param path
  * @param columns
  */
class RenameAssembler private[transforms] (path:Path, columns:Seq[(String,Path)]) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val node = root
            .find(path)
            .getOrElse(throw new IllegalArgumentException(s"Path $path not found in rename"))

        columns.flatMap(p => node.find(p._2).map(_.withName(p._1)))
    }
}

/**
  * This Assembler will explode a single column
  * @param path
  */
class ExplodeAssembler private[transforms] (name:String, path:Path) extends Assembler {
    override def reassemble[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val node = root
            .find(path)
            .getOrElse(throw new IllegalArgumentException(s"Path $path not found in explode"))

        val finalName = if (name.nonEmpty) name else node.name
        val elements = node match {
            case an:ArrayNode[T] => an.elements
            case n:Node[T] => n
        }

        Seq(elements.withName(finalName).withValue(ops.explode(finalName, node.mkValue())))
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
