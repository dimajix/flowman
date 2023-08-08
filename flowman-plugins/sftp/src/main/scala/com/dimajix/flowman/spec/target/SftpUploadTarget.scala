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

package com.dimajix.flowman.spec.target

import java.io.IOException
import java.net.URI
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.OpenMode
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.transport.verification.OpenSSHKnownHosts
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.tryWith
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.SshConnection


object SftpUploadTarget {
    def apply(context: Context, connection: Connection, source: Path, target: Path): SftpUploadTarget = {
        new SftpUploadTarget(
            Target.Properties(context, "sftp", "sftp"),
            source,
            target,
            ConnectionReference(context, connection),
            false,
            None,
            true
        )
    }
}

final case class SftpUploadTarget(
    instanceProperties:Target.Properties,
    source:Path,
    target:Path,
    connection: Reference[Connection],
    merge:Boolean,
    delimiter:Option[String],
    overwrite:Boolean
) extends BaseTarget {
    private lazy val sshCredentials = connection.value.asInstanceOf[SshConnection]
    private lazy val sshHost = sshCredentials.host
    private lazy val sshPort = if (sshCredentials.port > 0) sshCredentials.port else 22

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.CREATE, Phase.BUILD)

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.CREATE =>
                val src = context.fs.file(source)
                if (!src.exists()) {
                    Set()
                }
                else {
                    val dst = if (src.isDirectory() && !merge) target else target.getParent
                    Set(ResourceIdentifier.ofURI(new URI("sftp", null, sshHost, sshPort, dst.toString, null, null)))
                }
            case Phase.BUILD =>
                Set(ResourceIdentifier.ofURI(new URI("sftp", null, sshHost, sshPort, target.toString, null, null)))
            case _ => Set()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => Set(ResourceIdentifier.ofFile(source))
            case _ => Set()
        }
    }


    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        val src = execution.fs.file(source)

        phase match {
            case Phase.CREATE =>
                if (!src.exists()) {
                    No
                }
                else {
                    val dst = if (src.isDirectory() && !merge) target else target.getParent
                    withClient { client =>
                        !exists(client, dst)
                    }
                }
            case Phase.BUILD =>
                if (!src.exists()) {
                    No
                }
                else {
                    withClient { client =>
                        if (src.isDirectory() && !merge) {
                            isEmpty(client, target)
                        }
                        else {
                            !exists(client, target)
                        }
                    }
                }
            case _ => No
        }
    }


    /**
     * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
     * will not provide the data itself, it will only create the container. This method may throw an exception, which
     * will be caught an wrapped in the final [[TargetResult.]]
     *
     * @param execution
     */
    override protected def create(execution: Execution): Unit = {
        val src = execution.fs.file(source)
        if (src.exists()) {
            withClient { client =>
                if (src.isDirectory() && !merge) {
                    ensureDirectory(client, target)
                }
                else {
                    ensureDirectory(client, target.getParent)
                }
            }
        }
    }

    override protected def build(execution:Execution) : Unit = {
        val src = execution.fs.file(source)
        val dst = target
        val delimiter = this.delimiter.map(_.getBytes(Charset.forName("UTF-8")))
        logger.info(s"Uploading '$src' to remote destination 'sftp://$sshHost:$sshPort/$dst' (overwrite=$overwrite)")

        if (!src.exists()) {
            logger.error(s"Source '$src' does not exist")
            throw new IOException(s"Source '$src' does not exist")
        }

        withClient { client =>
            if (!overwrite && exists(client, dst)) {
                logger.error(s"Target file already exists at 'sftp://$sshHost:$sshPort/$dst'")
                throw new IOException(s"Target file already exists at 'sftp://$sshHost:$sshPort/$dst'")
            }

            if (src.isDirectory()) {
                if (merge)
                    uploadMergedFile(client, src, dst, delimiter)
                else
                    uploadDirectory(client, src, dst)
            }
            else {
                uploadSingleFile(client, src, dst)
            }
        }
    }

    private def uploadSingleFile(client:SFTPClient, src:com.dimajix.flowman.fs.File, dst:Path) : Unit = {
        logger.info(s"Uploading file '$src' to sftp remote destination '$dst'")
        ensureDirectory(client, dst.getParent)
        tryWith(src.open()) { input =>
            tryWith(client.open(dst.toString, Set(OpenMode.CREAT, OpenMode.TRUNC, OpenMode.WRITE).asJava)) { file =>
                tryWith(new file.RemoteFileOutputStream()) { output =>
                    IOUtils.copyBytes(input, output, 16384)
                }
            }
        }
    }

    private def uploadMergedFile(client:SFTPClient, src:com.dimajix.flowman.fs.File, dst:Path, delimiter:Option[Array[Byte]]) : Unit = {
        logger.info(s"Uploading merged directory '$src' to sftp remote destination '$dst'")
        ensureDirectory(client, dst.getParent)
        tryWith(client.open(dst.toString, Set(OpenMode.CREAT, OpenMode.TRUNC, OpenMode.WRITE).asJava)) { file =>
            tryWith(new file.RemoteFileOutputStream()) { output =>
                src.list()
                    .filter(_.isFile())
                    .sortBy(_.toString)
                    .foreach { file =>
                        tryWith(file.open()) { input =>
                            IOUtils.copyBytes(input, output, 16384)
                            delimiter.foreach(output.write)
                        }
                    }
            }
        }
    }

    private def uploadDirectory(client:SFTPClient, src:com.dimajix.flowman.fs.File, dst:Path) : Unit = {
        logger.info(s"Uploading directory '$src' to sftp remote destination '$dst'")
        ensureDirectory(client, dst)
        src.list()
            .filter(_.isFile())
            .foreach(file => {
                uploadSingleFile(client, file, new Path(dst, file.name))
            })
    }

    private def ensureDirectory(client: SFTPClient, path: Path) : Unit = {
        if (!exists(client, path)) {
            client.mkdirs(path.toString)
        }
    }

    private def isEmpty(client: SFTPClient, file: Path): Boolean = {
        !exists(client, file) || client.ls(file.toString).isEmpty
    }


    private def exists(client:SFTPClient, file:Path) : Boolean = {
        client.statExistence(file.toString) != null
    }

    private def withClient[T](fn:SFTPClient => T) : T= {
        withConnection { sshClient =>
            val sftpClient = sshClient.newSFTPClient()
            try {
                fn(sftpClient)
            }
            finally {
                sftpClient.close()
            }
        }
    }

    private def withConnection[T](fn:SSHClient => T) : T= {
        val username = sshCredentials.username
        val password = sshCredentials.password
        val keyFile = sshCredentials.keyFile
        val keyPassword = sshCredentials.keyPassword

        val hostKeyVerifier = sshCredentials.knownHosts match {
            case Some(kh) => new OpenSSHKnownHosts(kh)
            case None => new PromiscuousVerifier
        }

        logger.debug(s"Connecting via SFTP to $sshHost:$sshPort")
        val client = new SSHClient()
        client.addHostKeyVerifier(hostKeyVerifier)
        client.connect(sshHost, sshPort)
        val authMethods = client.getUserAuth.getAllowedMethods.asScala.toSet

        if (password.nonEmpty) {
            if (authMethods.contains("password")) {
                logger.debug(s"Using non-interactive password authentication for connecting to $sshHost:$sshPort")
                client.authPassword(username, password.orNull)
            }
            else {
                logger.error(s"Server at $sshHost:$sshPort does not support any of our supported password authentication methods")
                throw new IOException(s"Server at $sshHost:$sshPort does not support any of our supported password authentication methods")
            }
        }
        else {
            logger.debug(s"Using private key authentication for connecting to $sshHost:$sshPort")
            val keyProvider = keyPassword match {
                case Some(kpwd) => client.loadKeys(keyFile.map(_.toString).orNull, kpwd)
                case None => client.loadKeys(keyFile.map(_.toString).orNull)
            }
            client.authPublickey(username, keyProvider)
        }

        try {
            fn(client)
        }
        finally {
            client.close()
        }
    }
}




@TargetType(kind="sftpUpload")
class SftpUploadTargetSpec extends TargetSpec {
    @JsonProperty(value = "source", required = true) private var source: String = ""
    @JsonProperty(value = "target", required = true) private var target: String = ""
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "merge", required = false) private var merge: String = "false"
    @JsonProperty(value = "delimiter", required = true) private var delimiter: Option[String] = None
    @JsonProperty(value = "overwrite", required = false) private var overwrite: String = "true"

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): SftpUploadTarget = {
        SftpUploadTarget(
            instanceProperties(context, properties),
            new Path(context.evaluate(source)),
            new Path(context.evaluate(target)),
            connection.instantiate(context),
            context.evaluate(merge).toBoolean,
            context.evaluate(delimiter),
            context.evaluate(overwrite).toBoolean
        )
    }
}
