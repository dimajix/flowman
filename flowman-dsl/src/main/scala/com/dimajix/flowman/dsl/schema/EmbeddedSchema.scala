package com.dimajix.flowman.dsl.schema

import com.dimajix.flowman.dsl.SchemaGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema
import com.dimajix.flowman.types.Field


case class EmbeddedSchema(
    description : Option[String] = None,
    fields : Seq[Field] = Seq(),
    primaryKey : Seq[String] = Seq()
) extends SchemaGen {
    override def apply(props:Schema.Properties) : schema.EmbeddedSchema = {
        val env = props.context.environment
        schema.EmbeddedSchema(
            props,
            description,
            fields,
            primaryKey
        )
    }
}
