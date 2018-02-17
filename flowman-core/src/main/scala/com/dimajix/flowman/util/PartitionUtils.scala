package com.dimajix.flowman.util


object PartitionUtils {
    def foreach(partitions:Map[String,Iterable[String]], function:(Map[String,String]) => Unit) : Unit = {
        flatMap(partitions, map => {function(map); Seq()})
    }
    def flatMap[T](partitions:Map[String,Iterable[String]], function:(Map[String,String]) => Iterable[T]) : Iterable[T] = {
        flatMapPartitions(Map(), partitions, function)
    }
    def map[T](partitions:Map[String,Iterable[String]], function:(Map[String,String]) => T) : Iterable[T] = {
        flatMapPartitions(Map(), partitions, p => Some(function(p)))
    }

    private def flatMapPartitions[T](headPartitions:Map[String,String], tailPartitions:Map[String,Iterable[String]], function:(Map[String,String]) => Iterable[T]) : Iterable[T] = {
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
