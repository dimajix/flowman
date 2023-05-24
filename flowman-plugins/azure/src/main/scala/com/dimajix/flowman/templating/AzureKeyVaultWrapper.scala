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

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClientBuilder
import com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
import org.slf4j.LoggerFactory

import com.dimajix.flowman.annotation.TemplateObject


@TemplateObject(name="AzureKeyVault")
class AzureKeyVaultWrapper {
    private def logger = LoggerFactory.getLogger(classOf[AzureKeyVaultWrapper])

    def getSecret(vaultName:String, secretName:String) : String = {
        try {
            TokenLibrary.getSecret(vaultName, secretName)
        }
        catch {
            case _:ClassNotFoundException|_:NoClassDefFoundError =>
                logger.warn("No TokenLibrary provided, falling back to Azure SDK to retrieve secret from Key Vault")
                getSecretViaSdk(vaultName, secretName)
        }
    }

    def getSecret(vaultName: String, secretName: String, linkedService:String): String = {
        try {
            TokenLibrary.getSecret(vaultName, secretName, linkedService)
        }
        catch {
            case _: ClassNotFoundException|_:NoClassDefFoundError =>
                logger.warn("No TokenLibrary provided, falling back to Azure SDK to retrieve secret from Key Vault")
                getSecretViaSdk(vaultName, secretName)
        }
    }

    private def getSecretViaSdk(vaultName:String, secretName:String) : String = {
        val keyVaultUri = "https://" + vaultName + ".vault.azure.net"
        val secretClient = new SecretClientBuilder()
            .vaultUrl(keyVaultUri)
            .credential(new DefaultAzureCredentialBuilder().build ())
            .buildClient();
        val secret = secretClient.getSecret(secretName)
        secret.getValue
    }
}
