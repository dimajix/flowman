package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty


trait MigratableRelationSpec { this: RelationSpec =>
    @JsonProperty(value = "migrationStrategy", required = false) protected var migrationStrategy: Option[String] = None
    @JsonProperty(value = "migrationPolicy", required = false) protected var migrationPolicy: Option[String] = None
}
