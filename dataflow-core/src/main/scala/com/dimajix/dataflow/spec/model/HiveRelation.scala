package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.model.Relation.Partition

class HiveRelation extends Relation  {
    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = _
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = Seq()

    override def read(context:Context, schema:StructType, partition:Seq[Partition] = Seq()) : DataFrame = null
    override def write(context:Context, df:DataFrame, partition:Partition = null) : Unit = Unit
    override def create(context:Context) : Unit = Unit
    override def destroy(context:Context) : Unit = Unit
    override def migrate(context: Context) : Unit = Unit
}
