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

package com.dimajix.flowman.transforms.schema

import java.util.Locale


object Path {
    /**
      * Create an empty column path, which represents the root structure
      * @return
      */
    def apply() : Path = Path(Seq())

    /**
      * Creates a Path from a string which should contain the column names in dotted notation (some.nested.column)
      * @param path
      * @return
      */
    def apply(path:String) : Path = Path(path.split('.'))
}

/**
  * Small helper class to represent a column path within a nested structure
  */
case class Path(segments:Seq[String]) {
    override def toString: String = {
        if (segments.isEmpty)
            "."
        else
            segments.mkString(".")
    }
}


/**
  * Interface class required for recreating a native value (like a Spark schema) from an abstract tree
  * representation
  * @tparam T
  */
trait NodeOps[T] {
    def empty : T

    def metadata(value:T, meta:Map[String,String]) : T

    def leaf(name:String, value:T, nullable:Boolean) : T

    def struct(name:String, children:Seq[T], nullable:Boolean) : T

    def struct_pruned(name:String, children:Seq[T], nullable:Boolean) : T

    def array(name:String, element:T, nullable:Boolean) : T

    def map(name:String, keyType:T, valueType:T, nullable:Boolean) : T

    def explode(name:String, array:T) : T
}


/**
  * Base class for representing a schema as a tree with associated typical tree operations to support
  * reassembling a schema
  * @tparam T
  */
sealed abstract class Node[T] {
    /**
      * The (non-fully-qualified) name of this node
      * @return
      */
    def name : String

    /**
      * List of all children
      * @return
      */
    def children : Seq[Node[T]]

    /**
      * Returns true if the node contains a child with the specified name
      * @param node
      * @return
      */
    def contains(node:String) : Boolean

    /**
      * Retrieves a named child node of this tree (or None if no child was found)
      * @param node
      * @return
      */
    def get(node:String) : Option[Node[T]]

    /**
      * Walks down the specified path od this tree and returns the corresponding node
      * @param node
      * @return
      */
    def find(node:Path) : Option[Node[T]]

    /**
      * Returns true if this field should be nullable
      * @return
      */
    def nullable : Boolean

    /**
      * Returns the meta data of this field
      * @return
      */
    def metadata : Map[String,String]

    /**
      * Creates an appropriate value associated with this node
      * @return
      */
    def mkValue()(implicit ops:NodeOps[T]) : T

    /**
      * Creates a copy of the structure with a new explicit value
      * @param newValue
      * @return
      */
    def withValue(newValue:T) : Node[T]

    /**
      * Creates a copy of the structure with a new name of the current element
      * @param newName
      * @return
      */
    def withName(newName:String) : Node[T]

    /**
      * Creates a new node with the nullable property set accordingly to the parameter
      * @param n
      * @return
      */
    def withNullable(n:Boolean) : Node[T]

    /**
      * Creates a new node with the description set accordingly to the parameter
      * @param meta
      * @return
      */
    def withMetadata(meta:Map[String,String]) : Node[T]

    /**
      * Drops the specified path and returns a new subtree representing the pruned tree
      * @param path
      * @return
      */
    def drop(path:Path) : Node[T]

    def drop(paths:Seq[Path]) : Node[T] = {
        paths.foldLeft(this)((node,path) => node.drop(path))
    }

    /**
      * Prunes away everything except the specified paths (and their subtrees) and returns a new pruned tree
      * @param paths
      * @return
      */
    def keep(paths:Seq[Path]) : Node[T]

    protected def applyProperties(value:T)(implicit ops: NodeOps[T]) : T = {
        ops.metadata(value, metadata)
    }
}


case class LeafNode[T](name:String, value:T, nullable:Boolean=true, metadata:Map[String,String]=Map()) extends Node[T] {
    override def children : Seq[Node[T]] = Seq()

    /**
      * Returns true if the node contains a child with the specified name
      * @param node
      * @return
      */
    override def contains(node:String) : Boolean = {
        node.isEmpty
    }

    /**
      * Retrieves a named child node of this tree (or None if no child was found)
      * @param node
      * @return
      */
    override def get(node:String) : Option[Node[T]] = {
        if (node.isEmpty)
            Some(this)
        else
            None
    }

    /**
      * Walks down the specified path od this tree and returns the corresponding node
      * @param node
      * @return
      */
    override def find(node:Path): Option[Node[T]] = {
        if (node.segments.isEmpty)
            Some(this)
        else
            None
    }

    /**
      * Creates an appropriate value associated with this node
      * @return
      */
    override def mkValue()(implicit ops:NodeOps[T]) : T = {
        ops.leaf(name, applyProperties(value), nullable)
    }

    /**
      * Creates a copy of the structure with a new explicit value
      * @param newValue
      * @return
      */
    override def withValue(newValue:T) : LeafNode[T] = {
        if (newValue != value)
            copy(value=newValue)
        else
            this
    }

    /**
      * Creates a copy of the structure with a new name of the current element
      * @param newName
      * @return
      */
    override def withName(newName:String) : LeafNode[T] = {
        if (name != newName)
            copy(name=newName)
        else
            this
    }

    /**
      * Returns a copy of this node with the nullable value set to the specified value
      * @param n
      * @return
      */
    override def withNullable(n:Boolean) : Node[T] = {
        if (nullable != n)
            copy(nullable=n)
        else
            this
    }

    /**
      * Returns a copy of this node with the meta data set to the specified value
      * @param meta
      * @return
      */
    override def withMetadata(meta:Map[String,String]) : Node[T] = {
        if (metadata != meta)
            copy(metadata=meta)
        else
            this
    }

    override def drop(path:Path) : LeafNode[T] = this

    override def keep(paths:Seq[Path]) : LeafNode[T] = this
}


case class StructNode[T](name:String, value:Option[T], children:Seq[Node[T]], nullable:Boolean=true, metadata:Map[String,String]=Map()) extends Node[T] {
    private val nameToChild = children.map(node => (node.name.toLowerCase(Locale.ROOT), node)).toMap

    /**
      * Returns true if the node contains a child with the specified name
      * @param node
      * @return
      */
    override def contains(node:String) : Boolean = {
        if (node.isEmpty)
            true
        else
            nameToChild.contains(node.toLowerCase(Locale.ROOT))
    }

    /**
      * Retrieves a named child node of this tree (or None if no child was found)
      * @param node
      * @return
      */
    override def get(node:String) : Option[Node[T]] = {
        if (node.isEmpty)
            Some(this)
        else
            nameToChild.get(node.toLowerCase(Locale.ROOT))
    }

    /**
      * Walks down the specified path od this tree and returns the corresponding node
      * @param node
      * @return
      */
    override def find(node:Path): Option[Node[T]] = {
        val segments = node.segments
        if (segments.isEmpty) {
            Some(this)
        }
        else {
            val head = segments.head
            val tail = segments.tail
            get(head).flatMap(_.find(Path(tail)))
        }
    }

    /**
      * Creates an appropriate value associated with this node
      * @return
      */
    override def mkValue()(implicit ops:NodeOps[T]) : T = {
        val value = this.value
            .map(v => ops.leaf(name, v, nullable))
            .getOrElse {
                if (children.forall(_.nullable)) {
                    ops.struct_pruned(name, children.map(_.mkValue()), nullable)
                }
                else {
                    ops.struct(name, children.map(_.mkValue()), nullable)
                }
            }
        applyProperties(value)
    }

    /**
      * Creates a copy of the structure with a new explicit value
      * @param newValue
      * @return
      */
    override def withValue(newValue:T) : StructNode[T] = {
        val optValue = Option(newValue)
        if (value != optValue)
            copy(value=optValue)
        else
            this
    }

    /**
      * Creates a copy of the structure with a new name of the current element
      * @param newName
      * @return
      */
    override def withName(newName:String) : StructNode[T] = {
        if (name != newName)
            copy(name=newName)
        else
            this
    }

    /**
      * Returns a copy of this node with the nullable value set to the specified value
      * @param n
      * @return
      */
    override def withNullable(n:Boolean) : StructNode[T] = {
        if (nullable != n)
            copy(nullable=n)
        else
            this
    }

    /**
      * Returns a copy of this node with the meta data set to the specified value
      * @param meta
      * @return
      */
    override def withMetadata(meta:Map[String,String]) : StructNode[T] = {
        if (meta != metadata)
            copy(metadata=meta)
        else
            this
    }

    /**
      * Drops the specified path and returns a new subtree representing the pruned tree
      * @param path
      * @return
      */
    override def drop(path:Path) : StructNode[T] = {
        require(path.segments.nonEmpty)
        val segments = path.segments
        val head = segments.head.toLowerCase(Locale.ROOT)
        val tail = segments.tail
        val newChildren = children.flatMap(child =>
            if (child.name.toLowerCase(Locale.ROOT) == head) {
                if (tail.isEmpty)
                    None
                else
                    Some(child.drop(Path(tail)))
            }
            else {
                Some(child)
            }
        )
        replaceChildren(newChildren)
    }

    /**
      * Prunes away everything except the specified paths (and their subtrees) and returns a new pruned tree
      * @param paths
      * @return
      */
    override def keep(paths:Seq[Path]) : StructNode[T] = {
        if (paths.exists(_.segments.isEmpty)) {
            // Special case: One path was empty, which implies we keep everything
            this
        }
        else {
            val ht = paths.foldLeft(Map[String,Seq[Path]]()) { (map, path) =>
                val head = path.segments.head
                if (contains(head)) {
                    val tail = Path(path.segments.tail)
                    val paths = map.get(head).map(_ :+ tail).getOrElse(Seq(tail))
                    map.updated(head, paths)
                }
                else {
                    map
                }
            }
            val newChildren = ht.map { case (head, tails) =>
                get(head).get.keep(tails)
            }
            replaceChildren(newChildren.toSeq)
        }
    }

    private def replaceChildren(newChildren:Seq[Node[T]]) : StructNode[T] = {
        // Check if something has changed
        if (newChildren.length != children.length || children.zip(newChildren).exists(xy => !(xy._1 eq xy._2))) {
            copy(value=None, children=newChildren)
        }
        else {
            this
        }
    }
}


case class ArrayNode[T](name:String, value:Option[T], elements:Node[T], nullable:Boolean=true, metadata:Map[String,String]=Map()) extends Node[T] {
    override def children : Seq[Node[T]] = elements.children

    /**
      * Returns true if the node contains a child with the specified name
      * @param node
      * @return
      */
    override def contains(node:String) : Boolean = {
        if (node.isEmpty)
            true
        else
            elements.contains(node)
    }

    /**
      * Retrieves a named child node of the contained element (or None if no child was found)
      * @param node
      * @return
      */
    override def get(node:String) : Option[Node[T]] = {
        if (node.isEmpty)
            Some(this)
        else
            elements.get(node)
    }

    /**
      * Walks down the specified path od this tree and returns the corresponding node
      * @param node
      * @return
      */
    override def find(node:Path): Option[Node[T]] = {
        if (node.segments.isEmpty)
            Some(this)
        else
            elements.find(node)
    }

    /**
      * Creates an appropriate value associated with this node
      * @return
      */
    override def mkValue()(implicit ops:NodeOps[T]) : T = {
        val value = this.value
            .map(v => ops.leaf(name, v, nullable))
            .getOrElse(ops.array(name, elements.mkValue(), nullable))
        applyProperties(value)
    }

    /**
      * Creates a copy of the structure with a new explicit value
      * @param newValue
      * @return
      */
    override def withValue(newValue:T) : ArrayNode[T] = {
        val optValue = Option(newValue)
        if (value != optValue)
            copy(value=optValue)
        else
            this
    }

    /**
      * Creates a copy of the structure with a new name of the current element
      * @param newName
      * @return
      */
    override def withName(newName:String) : ArrayNode[T] = {
        if (newName != name)
            copy(name=newName)
        else
            this
    }

    /**
      * Returns a copy of this node with the nullable value set to the specified value
      * @param n
      * @return
      */
    override def withNullable(n:Boolean) : ArrayNode[T] = {
        if (n != nullable)
            copy(nullable=n)
        else
            this
    }

    /**
      * Returns a copy of this node with the meta data set to the specified value
      * @param meta
      * @return
      */
    override def withMetadata(meta:Map[String,String]) : ArrayNode[T] = {
        if (meta != metadata)
            copy(metadata=meta)
        else
            this
    }

    override def drop(path:Path) : ArrayNode[T] = {
        require(path.segments.nonEmpty)
        val prunedElements = elements.drop(path)
        replaceElements(prunedElements)
    }

    override def keep(paths:Seq[Path]) : ArrayNode[T] = {
        val prunedElements = elements.keep(paths)
        replaceElements(prunedElements)
    }

    private def replaceElements(newElements:Node[T]) : ArrayNode[T] = {
        if (newElements eq elements) {
            this
        }
        else {
            copy(value=None, elements=newElements)
        }
    }
}


case class MapNode[T](name:String, value:Option[T], mapKey:Node[T], mapValue:Node[T], nullable:Boolean=true, metadata:Map[String,String]=Map()) extends Node[T] {
    override def children : Seq[Node[T]] = mapValue.children

    /**
      * Returns true if the node contains a child with the specified name
      * @param node
      * @return
      */
    override def contains(node:String) : Boolean = {
        if (node.isEmpty)
            true
        else
            mapValue.contains(node)
    }

    /**
      * Retrieves a named child node of the contained element (or None if no child was found)
      * @param node
      * @return
      */
    override def get(node:String) : Option[Node[T]] = {
        if (node.isEmpty)
            Some(this)
        else
            mapValue.get(node)
    }

    /**
      * Walks down the specified path od this tree and returns the corresponding node
      * @param node
      * @return
      */
    override def find(node:Path): Option[Node[T]] = {
        if (node.segments.isEmpty)
            Some(this)
        else
            mapValue.find(node)
    }

    /**
      * Creates an appropriate value associated with this node
      * @return
      */
    override def mkValue()(implicit ops:NodeOps[T]) : T = {
        val value = this.value
            .map(v => ops.leaf(name, v, nullable))
            .getOrElse(ops.map(name, mapKey.mkValue(), mapValue.mkValue(), nullable))
        applyProperties(value)
    }

    /**
      * Creates a copy of the structure with a new explicit value
      * @param newValue
      * @return
      */
    override def withValue(newValue:T) : MapNode[T] = {
        val optValue = Option(newValue)
        if (value != optValue)
            copy(value=optValue)
        else
            this
    }

    /**
      * Creates a copy of the structure with a new name of the current element
      * @param newName
      * @return
      */
    override def withName(newName:String) : MapNode[T] = {
        if (name != newName)
            copy(name=newName)
        else
            this
    }

    /**
      * Returns a copy of this node with the nullable value set to the specified value
      * @param n
      * @return
      */
    override def withNullable(n:Boolean) : MapNode[T] = {
        if (nullable != n)
            copy(nullable=n)
        else
            this
    }

    /**
      * Returns a copy of this node with the meta data set to the specified value
      * @param meta
      * @return
      */
    override def withMetadata(meta:Map[String,String]) : MapNode[T] = {
        if (meta != metadata)
            copy(metadata=meta)
        else
            this
    }

    override def drop(path:Path) : MapNode[T] = {
        require(path.segments.nonEmpty)
        val prunedElements = mapValue.drop(path)
        replaceElements(mapKey, prunedElements)
    }

    override def keep(paths:Seq[Path]) : MapNode[T] = {
        val prunedElements = mapValue.keep(paths)
        replaceElements(mapKey, prunedElements)
    }

    private def replaceElements(newKey:Node[T], newValue:Node[T]) : MapNode[T] = {
        if ((newKey eq mapKey) && (newValue eq mapValue)) {
            this
        }
        else {
            MapNode(name, None, newKey, newValue)
        }
    }
}
