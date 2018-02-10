package com.dimajix.dataflow.util


object PartitionUtils {
    def foreach(partitions:Map[String,Iterable[String]], function:(Map[String,String]) => Unit) : Unit = {
        flatMap(partitions, map => {function(map); Seq()})
    }
    def flatMap[T](partitions:Map[String,Iterable[String]], function:(Map[String,String]) => Iterable[T]) : Iterable[T] = {
        flatMapRec(Map(), partitions, function)
    }

    private def flatMapRec[T](headPartitions:Map[String,String], tailPartitions:Map[String,Iterable[String]], function:(Map[String,String]) => Iterable[T]) : Iterable[T] = {
        if (tailPartitions.nonEmpty) {
            val head = tailPartitions.head
            val tail = tailPartitions.tail
            head._2.flatMap(value => {
                val prefix = headPartitions.updated(head._1, value)
                flatMapRec(prefix, tail, function)
            })
        }
        else {
            Seq()
        }
    }

}
