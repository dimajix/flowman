/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Profile
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Template


class RootContextTest extends AnyFlatSpec with Matchers with MockFactory {
    "The RootContext" should "apply profiles" in {
        val namespace = Namespace(
            name = "default",
            profiles = Map(
                "profile" -> Profile(name="profile")
            )
        )
        val project = Project(
            name = "my_project",
            profiles = Map(
                "profile" -> Profile(name="profile")
            )
        )

        val session = Session.builder()
            .withNamespace(namespace)
            .withProfile("profile")
            .withProfile("profile2")
            .build()

        val rootContext = session.context
        rootContext.profiles should be (Set("profile", "profile2"))

        val projectContext = session.getContext(project)
        projectContext.profiles should be (Set("profile", "profile2"))
    }

    it should "correctly lookup connections in profiles" in {
        val namespaceConnectionTemplate = mock[Template[Connection]]
        val namespaceConnection = mock[Connection]
        val namespaceProfileConnectionTemplate = mock[Template[Connection]]
        val namespaceProfileConnection = mock[Connection]
        val namespace = Namespace(
            name = "default",
            connections = Map("con_namespace" -> namespaceConnectionTemplate),
            profiles = Map(
                "profile" -> Profile(
                    name="profile",
                    connections = Map("con_namespace_profile" -> namespaceProfileConnectionTemplate)
                )
            )
        )

        val projectConnectionTemplate = mock[Template[Connection]]
        val projectConnection = mock[Connection]
        val projectProfileConnectionTemplate = mock[Template[Connection]]
        val projectProfileConnection = mock[Connection]
        val project = Project(
            name = "my_project",
            connections = Map("con_project" -> projectConnectionTemplate),
            profiles = Map(
                "profile" -> Profile(
                    name="profile",
                    connections = Map("con_project_profile" -> projectProfileConnectionTemplate)
                )
            )
        )

        val session = Session.builder()
            .withNamespace(namespace)
            .withProfile("profile")
            .withProfile("profile2")
            .build()

        // Access everything via root context
        val rootContext = session.context
        (namespaceConnectionTemplate.instantiate _).expects(rootContext).returns(namespaceConnection)
        rootContext.getConnection(ConnectionIdentifier("con_namespace")) should be (namespaceConnection)
        (namespaceProfileConnectionTemplate.instantiate _).expects(rootContext).returns(namespaceProfileConnection)
        rootContext.getConnection(ConnectionIdentifier("con_namespace_profile")) should be (namespaceProfileConnection)
        a[NoSuchConnectionException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("con_project")))
        a[NoSuchConnectionException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("con_project_profile")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_project")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_namespace")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_namespace_profile")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_project_profile")))

        // Access everything via project context
        val projectContext = session.getContext(project)
        projectContext.getConnection(ConnectionIdentifier("con_namespace")) should be (namespaceConnection)
        projectContext.getConnection(ConnectionIdentifier("con_namespace_profile")) should be (namespaceProfileConnection)
        (projectConnectionTemplate.instantiate _).expects(projectContext).returns(projectConnection)
        projectContext.getConnection(ConnectionIdentifier("con_project")) should be (projectConnection)
        (projectProfileConnectionTemplate.instantiate _).expects(projectContext).returns(projectProfileConnection)
        projectContext.getConnection(ConnectionIdentifier("con_project_profile")) should be (projectProfileConnection)
        projectContext.getConnection(ConnectionIdentifier("my_project/con_project")) should be (projectConnection)
        a[NoSuchConnectionException] should be thrownBy (projectContext.getConnection(ConnectionIdentifier("my_project/con_namespace")))
        a[NoSuchConnectionException] should be thrownBy (projectContext.getConnection(ConnectionIdentifier("my_project/con_namespace_profile")))
        projectContext.getConnection(ConnectionIdentifier("my_project/con_project_profile")) should be (projectProfileConnection)
        a[NoSuchProjectException] should be thrownBy (projectContext.getConnection(ConnectionIdentifier("no_such_project/con_project_profile")))

        // Again try to access project resources after its context has been created
        rootContext.getConnection(ConnectionIdentifier("my_project/con_project")) should be (projectConnection)
        a[NoSuchConnectionException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_namespace")))
        a[NoSuchConnectionException] should be thrownBy (rootContext.getConnection(ConnectionIdentifier("my_project/con_namespace_profile")))
        rootContext.getConnection(ConnectionIdentifier("my_project/con_project_profile")) should be (projectProfileConnection)
    }

    it should "support override mappings" in {
        val projectMapping1 = mock[Mapping]
        val projectMappingTemplate1 = mock[Template[Mapping]]
        val projectMapping2 = mock[Mapping]
        val projectMappingTemplate2 = mock[Template[Mapping]]
        val overrideMapping = mock[Mapping]
        val overrideMappingTemplate = mock[Template[Mapping]]

        val project = Project(
            name = "my_project",
            mappings = Map(
                "m1" -> projectMappingTemplate1,
                "m2" -> projectMappingTemplate2
            )
        )

        val session = Session.builder()
            .withProfile("profile")
            .build()
        val rootContext = RootContext.builder(session.context)
            .overrideMappings(Map(MappingIdentifier("my_project/m2") -> overrideMappingTemplate))
            .build()

        // Access everything via root context
        a[NoSuchMappingException] should be thrownBy (rootContext.getMapping(MappingIdentifier("m1")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getMapping(MappingIdentifier("my_project/m1")))
        a[NoSuchMappingException] should be thrownBy (rootContext.getMapping(MappingIdentifier("m2")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getMapping(MappingIdentifier("my_project/m2")))

        // Access everything via project context
        val projectContext = rootContext.getProjectContext(project)
        (projectMappingTemplate1.instantiate _).expects(projectContext).returns(projectMapping1)
        projectContext.getMapping(MappingIdentifier("m1")) should be (projectMapping1)
        projectContext.getMapping(MappingIdentifier("my_project/m1")) should be (projectMapping1)
        projectContext.getMapping(MappingIdentifier("m1"), false) should be (projectMapping1)
        projectContext.getMapping(MappingIdentifier("my_project/m1"), false) should be (projectMapping1)
        (overrideMappingTemplate.instantiate _).expects(projectContext).returns(overrideMapping)
        projectContext.getMapping(MappingIdentifier("m2")) should be (overrideMapping)
        projectContext.getMapping(MappingIdentifier("my_project/m2")) should be (overrideMapping)
        (projectMappingTemplate2.instantiate _).expects(projectContext).returns(projectMapping2)
        projectContext.getMapping(MappingIdentifier("m2"), false) should be (projectMapping2)
        projectContext.getMapping(MappingIdentifier("my_project/m2"), false) should be (projectMapping2)

        // Again try to access project resources after its context has been created
        a[NoSuchMappingException] should be thrownBy (rootContext.getMapping(MappingIdentifier("m1")))
        a[NoSuchMappingException] should be thrownBy (rootContext.getMapping(MappingIdentifier("m2")))
        rootContext.getMapping(MappingIdentifier("my_project/m1")) should be (projectMapping1)
        rootContext.getMapping(MappingIdentifier("my_project/m1"), false) should be (projectMapping1)
        rootContext.getMapping(MappingIdentifier("my_project/m2")) should be (overrideMapping)
        rootContext.getMapping(MappingIdentifier("my_project/m2"), false) should be (projectMapping2)
    }

    it should "support override relations" in {
        val projectRelation1 = mock[Relation]
        val projectRelationTemplate1 = mock[Template[Relation]]
        val projectRelation2 = mock[Relation]
        val projectRelationTemplate2 = mock[Template[Relation]]
        val overrideRelation = mock[Relation]
        val overrideRelationTemplate = mock[Template[Relation]]

        val project = Project(
            name = "my_project",
            relations = Map(
                "m1" -> projectRelationTemplate1,
                "m2" -> projectRelationTemplate2
            )
        )

        val session = Session.builder()
            .withProfile("profile")
            .build()
        val rootContext = RootContext.builder(session.context)
            .overrideRelations(Map(RelationIdentifier("my_project/m2") -> overrideRelationTemplate))
            .build()

        // Access everything via root context
        a[NoSuchRelationException] should be thrownBy (rootContext.getRelation(RelationIdentifier("m1")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getRelation(RelationIdentifier("my_project/m1")))
        a[NoSuchRelationException] should be thrownBy (rootContext.getRelation(RelationIdentifier("m2")))
        a[NoSuchProjectException] should be thrownBy (rootContext.getRelation(RelationIdentifier("my_project/m2")))

        // Access everything via project context
        val projectContext = rootContext.getProjectContext(project)
        (projectRelationTemplate1.instantiate _).expects(projectContext).returns(projectRelation1)
        projectContext.getRelation(RelationIdentifier("m1")) should be (projectRelation1)
        projectContext.getRelation(RelationIdentifier("my_project/m1")) should be (projectRelation1)
        projectContext.getRelation(RelationIdentifier("m1"), false) should be (projectRelation1)
        projectContext.getRelation(RelationIdentifier("my_project/m1"), false) should be (projectRelation1)
        (overrideRelationTemplate.instantiate _).expects(projectContext).returns(overrideRelation)
        projectContext.getRelation(RelationIdentifier("m2")) should be (overrideRelation)
        projectContext.getRelation(RelationIdentifier("my_project/m2")) should be (overrideRelation)
        (projectRelationTemplate2.instantiate _).expects(projectContext).returns(projectRelation2)
        projectContext.getRelation(RelationIdentifier("m2"), false) should be (projectRelation2)
        projectContext.getRelation(RelationIdentifier("my_project/m2"), false) should be (projectRelation2)

        // Again try to access project resources after its context has been created
        a[NoSuchRelationException] should be thrownBy (rootContext.getRelation(RelationIdentifier("m1")))
        a[NoSuchRelationException] should be thrownBy (rootContext.getRelation(RelationIdentifier("m2")))
        rootContext.getRelation(RelationIdentifier("my_project/m1")) should be (projectRelation1)
        rootContext.getRelation(RelationIdentifier("my_project/m1"), false) should be (projectRelation1)
        rootContext.getRelation(RelationIdentifier("my_project/m2")) should be (overrideRelation)
        rootContext.getRelation(RelationIdentifier("my_project/m2"), false) should be (projectRelation2)
    }
}
