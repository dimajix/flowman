/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.Context


sealed abstract class Reference[T] {
    val value:T
    def name:String
    def identifier:Identifier[T]
}


object RelationReference {
    def apply(context:Context, prototype:Prototype[Relation]) : ValueRelationReference =
        ValueRelationReference(context, prototype)
    def apply(context: Context, relation:RelationIdentifier) : IdentifierRelationReference =
        IdentifierRelationReference(context, relation)
}
final case class ValueRelationReference(context:Context, prototype:Prototype[Relation]) extends Reference[Relation] {
    override lazy val value : Relation = prototype.instantiate(context)
    override def name: String = value.name
    override def identifier: RelationIdentifier = value.identifier
    override def toString: String = value.identifier.toString
}
object IdentifierRelationReference {
    def apply(context: Context, relation:String) : IdentifierRelationReference =
        IdentifierRelationReference(context, RelationIdentifier(relation))
}
final case class IdentifierRelationReference(context: Context, relation:RelationIdentifier) extends Reference[Relation] {
    override lazy val value: Relation = context.getRelation(relation)
    override def name: String = relation.name
    override def identifier: RelationIdentifier = relation
    override def toString: String = relation.toString
}


object ConnectionReference {
    def apply(context:Context, prototype:Prototype[Connection]) : ValueConnectionReference =
        ValueConnectionReference(context, prototype)
    def apply(context: Context, connection:ConnectionIdentifier) : IdentifierConnectionReference =
        IdentifierConnectionReference(context, connection)
}
final case class ValueConnectionReference(context:Context, prototype:Prototype[Connection]) extends Reference[Connection] {
    override lazy val value : Connection = prototype.instantiate(context)
    override def name: String = value.name
    override def identifier: ConnectionIdentifier = value.identifier
    override def toString: String = value.identifier.toString
}
object IdentifierConnectionReference {
    def apply(context: Context, connection:String) : IdentifierConnectionReference =
        IdentifierConnectionReference(context, ConnectionIdentifier(connection))
}
final case class IdentifierConnectionReference(context: Context, connection:ConnectionIdentifier) extends Reference[Connection] {
    override lazy val value: Connection = context.getConnection(connection)
    override def name: String = connection.name
    override def identifier: ConnectionIdentifier = connection
    override def toString: String = connection.toString
}

