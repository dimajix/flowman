/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.kernel.grpc

import java.io.File

import scala.collection.JavaConverters._

import io.grpc.stub.StreamObserver
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.fs.JavaFile
import com.dimajix.flowman.kernel.proto.workspace._
import com.dimajix.flowman.kernel.service.MultiWorkspaceManager
import com.dimajix.spark.testing.LocalTempDir


class WorkspaceServiceHandlerTest extends AnyFlatSpec with Matchers with MockFactory with LocalTempDir {
    "The WorkspaceServiceHandler" should "work" in {
        val rootPath = new File(tempDir, "wsm").toPath
        val root = JavaFile(rootPath)
        val wsm = new MultiWorkspaceManager(root)

        val handler = new WorkspaceServiceHandler(wsm)

        // List
        val observer1 = mock[StreamObserver[ListWorkspacesResponse]]
        def check1(r:ListWorkspacesResponse) : Unit = r.getWorkspacesList.asScala.map(_.getName) should be (Seq("default"))
        (observer1.onNext _).expects(*).onCall(check1 _)
        (observer1.onCompleted _).expects()
        handler.listWorkspaces(ListWorkspacesRequest.getDefaultInstance, observer1)

        // Create
        val observer2 = mock[StreamObserver[CreateWorkspaceResponse]]
        def check2(r:CreateWorkspaceResponse) : Unit = {}
        (observer2.onNext _).expects(*).onCall(check2 _)
        (observer2.onCompleted _).expects()
        handler.createWorkspace(CreateWorkspaceRequest.newBuilder().setName("test").build(), observer2)

        // Create again
        val observer3 = mock[StreamObserver[CreateWorkspaceResponse]]
        (observer3.onError _).expects(*)
        handler.createWorkspace(CreateWorkspaceRequest.newBuilder().setName("test").build(), observer3)

        // List again
        val observer4 = mock[StreamObserver[ListWorkspacesResponse]]
        def check4(r: ListWorkspacesResponse): Unit = r.getWorkspacesList.asScala.map(_.getName).toSet should be(Set("default","test"))
        (observer4.onNext _).expects(*).onCall(check4 _)
        (observer4.onCompleted _).expects()
        handler.listWorkspaces(ListWorkspacesRequest.getDefaultInstance, observer4)

        // Delete
        val observer5 = mock[StreamObserver[DeleteWorkspaceResponse]]
        def check5(r: DeleteWorkspaceResponse): Unit = {}
        (observer5.onNext _).expects(*).onCall(check5 _)
        (observer5.onCompleted _).expects()
        handler.deleteWorkspace(DeleteWorkspaceRequest.newBuilder().setWorkspaceId("test").build(), observer5)

        // Delete again
        val observer6 = mock[StreamObserver[DeleteWorkspaceResponse]]
        (observer6.onError _).expects(*)
        handler.deleteWorkspace(DeleteWorkspaceRequest.newBuilder().setWorkspaceId("test").build(), observer6)

        // List again
        val observer7 = mock[StreamObserver[ListWorkspacesResponse]]
        def check7(r: ListWorkspacesResponse): Unit = r.getWorkspacesList.asScala.map(_.getName).toSet should be(Set("default"))
        (observer7.onNext _).expects(*).onCall(check7 _)
        (observer7.onCompleted _).expects()
        handler.listWorkspaces(ListWorkspacesRequest.getDefaultInstance, observer7)
    }
}
