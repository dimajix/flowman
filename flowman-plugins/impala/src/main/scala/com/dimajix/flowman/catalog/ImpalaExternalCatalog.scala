/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.slf4j.LoggerFactory

import com.dimajix.flowman.jdbc.HiveDialect


object ImpalaExternalCatalog {
    val IMPALA_DEFAULT_DRIVER = "com.cloudera.impala.jdbc41.Driver"
    val IMPALA_DEFAULT_PORT = 21050

    case class Connection(
         url:String = "",
         host:String = "",
         port:Int = IMPALA_DEFAULT_PORT,
         driver:String = IMPALA_DEFAULT_DRIVER,
         user:String = "",
         password:String = "",
         properties: Map[String,String] = Map()
     )
}


class ImpalaExternalCatalog(connection:ImpalaExternalCatalog.Connection) extends ExternalCatalog {
    private val logger = LoggerFactory.getLogger(classOf[ImpalaExternalCatalog])
    private val connect = createConnectionFactory(connection)

    override def createTable(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for newly created table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def alterTable(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for modified table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"REFRESH METADATA $identifier")
        }
    }

    override def dropTable(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for dropped table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def truncateTable(table: CatalogTable): Unit = {
        logger.info(s"REFRESH Impala metadata for truncated table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"REFRESH METADATA $identifier")
        }
    }

    override def addPartition(table: CatalogTable, partition: CatalogTablePartition): Unit = {
        logger.info(s"REFRESH Impala metadata for new partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH METADATA $identifier $spec")
        }
    }

    override def alterPartition(table: CatalogTable, partition: CatalogTablePartition): Unit =  {
        logger.info(s"REFRESH Impala metadata for changed partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH METADATA $identifier $spec")
        }
    }

    override def dropPartition(table: CatalogTable, partition: CatalogTablePartition): Unit = {
        logger.info(s"INVALIDATE Impala metadata for dropped partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def truncatePartition(table: CatalogTable, partition: CatalogTablePartition): Unit =  {
        logger.info(s"REFRESH Impala metadata for truncated partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH METADATA $identifier $spec")
        }
    }

    private def withConnection[T](fn:Connection => T) : T = {
        val conn = connect()
        try {
            fn(conn)
        }
        finally {
            conn.close()
        }
    }

    private def withStatement[T](fn:Statement => T) : T = {
        withConnection { con =>
            val statement = con.createStatement()
            try {
                // statement.setQueryTimeout(options.queryTimeout)
                fn(statement)
            }
            finally {
                statement.close()
            }
        }
    }

    private def createConnectionFactory(options: ImpalaExternalCatalog.Connection): () => Connection = {
        val driver = Option(options.driver)
            .filter(_.nonEmpty)
            .getOrElse(ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER)
        val url = Option(options.url)
            .filter(_.nonEmpty)
            .getOrElse("jdbc:impala://" + options.host + ":" + options.port)

        Class.forName(driver)

        val properties = new Properties()
        Option(options.user).foreach(properties.setProperty("user", _))
        Option(options.password).foreach(properties.setProperty("password", _))
        options.properties.foreach(kv => properties.setProperty(kv._1, kv._2))
        () => {
            DriverManager.getConnection(url, properties)
        }
    }
}
