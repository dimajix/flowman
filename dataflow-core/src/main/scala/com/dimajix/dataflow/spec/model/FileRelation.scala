package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.model.Relation.Partition


class FileRelation extends BaseRelation {
    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = _
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = Seq()
    @JsonProperty(value="pattern") private var _pattern: Seq[Field] = Seq()

    override def read(executor:Executor, schema:StructType, partition:Seq[Partition] = Seq()) : DataFrame = ???
    override def write(executor:Executor, df:DataFrame, partition:Partition, mode:String) : Unit = ???
    override def create(executor:Executor) : Unit = ???
    override def destroy(executor:Executor) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???
}
