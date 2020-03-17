package com.dimajix.flowman.dsl.schema

import com.dimajix.flowman.dsl.SchemaGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema
import com.dimajix.flowman.types.Field


case class EmbeddedSchema(
    description : Environment => Option[String] = _ => None,
    fields : Environment => Seq[Field] = _ => Seq(),
    primaryKey : Environment => Seq[String] = _ => Seq()
) extends SchemaGen {
    override def apply(props:Schema.Properties) : schema.EmbeddedSchema = {
        ???
    }
}
