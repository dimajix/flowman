package com.dimajix.flowman.dsl.mapping

import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.mapping.ExtractJsonMapping


case class ExtractJson(
    input:Environment => MappingOutputIdentifier,
    column:Environment => String,
    schema:Template[Schema],
    parseMode:Environment => String = _ => "PERMISSIVE"
) extends MappingGen {
    def apply(props:Mapping.Properties) : ExtractJsonMapping = {
        val env = props.context.environment
        ExtractJsonMapping(
            props,
            input(env),
            column(env),
            schema.instantiate(props.context),
            parseMode(env)
        )
    }
}
