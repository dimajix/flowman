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

package com.dimajix.flowman.util


object PartitionUtils {
    def foreach(partitions:Map[String,Iterable[Any]], function:(Map[String,Any]) => Unit) : Unit = {
        flatMap(partitions, map => {function(map); Seq()})
    }
    def flatMap[T](partitions:Map[String,Iterable[Any]], function:(Map[String,Any]) => Iterable[T]) : Iterable[T] = {
        flatMapPartitions(Map(), partitions, function)
    }
    def map[T](partitions:Map[String,Iterable[Any]], function:(Map[String,Any]) => T) : Iterable[T] = {
        flatMapPartitions(Map(), partitions, p => Some(function(p)))
    }

    private def flatMapPartitions[T](headPartitions:Map[String,Any], tailPartitions:Map[String,Iterable[Any]], function:(Map[String,Any]) => Iterable[T]) : Iterable[T] = {
        if (tailPartitions.nonEmpty) {
            val head = tailPartitions.head
            val tail = tailPartitions.tail
            head._2.flatMap(value => {
                val prefix = headPartitions.updated(head._1, value)
                flatMapPartitions(prefix, tail, function)
            })
        }
        else {
            function(headPartitions)
        }
    }
}
