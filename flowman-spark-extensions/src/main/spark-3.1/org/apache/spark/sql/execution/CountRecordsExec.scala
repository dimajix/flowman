package com.dimajix.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator


case class CountRecordsExec(child: SparkPlan, counter:LongAccumulator) extends UnaryExecNode {
    override def output: Seq[Attribute] = child.output

    override def outputPartitioning: Partitioning = child.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = child.outputOrdering

    override def supportsColumnar: Boolean = child.supportsColumnar
    override def vectorTypes: Option[Seq[String]] = child.vectorTypes

    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
        val c = counter
        child.executeColumnar().mapPartitions { iter =>
            iter.map { batch =>
                c.add(batch.numRows())
                batch
            }
        }
    }

    override protected def doExecute(): RDD[InternalRow] = {
        val c = counter
        child.execute().mapPartitions { iter =>
            iter.map { row =>
                c.add(1)
                row
            }
        }
    }
}
