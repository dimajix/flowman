/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.templating

import scala.collection.mutable

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest

import com.dimajix.flowman.annotation.TemplateObject


object AwsSecretsManagerWrapper {
    private val clients = mutable.Map[String,AWSSecretsManager]()
    def client(region:String) : AWSSecretsManager = {
        clients.synchronized {
            clients.getOrElseUpdate(region, createClient(region))
        }
    }

    private def createClient(region:String) : AWSSecretsManager = {
        val endpoint = "secretsmanager." + region + ".amazonaws.com"

        val config = new AwsClientBuilder.EndpointConfiguration(endpoint, region)
        val clientBuilder = AWSSecretsManagerClientBuilder.standard
        clientBuilder.setCredentials(new DefaultAWSCredentialsProviderChain)
        clientBuilder.setEndpointConfiguration(config)

        clientBuilder.build()
    }
}


@TemplateObject(name = "AwsSecretsManager")
class AwsSecretsManagerWrapper {
    def getSecret(secretName: String, region:String): String = {
        val client = AwsSecretsManagerWrapper.client(region)
        val getSecretValueRequest = new GetSecretValueRequest()
            .withSecretId(secretName)
            .withVersionStage("AWSCURRENT")
        val getSecretValueResult = client.getSecretValue(getSecretValueRequest)

        if (getSecretValueResult == null) {
            null
        }
        // Depending on whether the secret was a string or binary, one of these fields will be populated
        else if (getSecretValueResult.getSecretString != null) {
            getSecretValueResult.getSecretString
        }
        else {
            val binarySecretData = getSecretValueResult.getSecretBinary
            binarySecretData.toString
        }
    }
    def getSecret(secretName: String, keyName:String, region:String): String = {
        val secret = getSecret(secretName, region)
        JsonWrapper.path(secret, "$." + keyName).toString
    }
}
