# Test Suite for Amazon EMR

Jar File: s3://flowman-test/integration-tests/emr/emr-test-1.0-SNAPSHOT-emr.jar
Main class: com.dimajix.flowman.tools.exec.Driver
Arguments: ["-B","-f","flow","job","build","main","--force"]

## Required Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadAccessForEMRSamples",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::*.elasticmapreduce",
                "arn:aws:s3:::*.elasticmapreduce/*"   
            ]
        },
        {
            "Sid": "ReadWriteAccessForFlowman",
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*",
                "s3:*Object*"
            ],
            "Resource": [
                "arn:aws:s3:::flowman-test/*",
                "arn:aws:s3:::flowman-test"
            ]
        },
        {
            "Sid": "GlueCreateAndReadDataCatalog",
            "Effect": "Allow",
            "Action": [
                "glue:BatchCreatePartition",
                "glue:BatchDeletePartition",
                "glue:BatchGetPartition",
                "glue:CreateDatabase",
                "glue:CreateTable",
                "glue:CreateUserDefinedFunction",
                "glue:DeleteDatabase",
                "glue:DeletePartition",
                "glue:DeleteTable",
                "glue:DeleteUserDefinedFunction",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetUserDefinedFunction",
                "glue:GetUserDefinedFunctions",
                "glue:UpdateDatabase",
                "glue:UpdatePartition",
                "glue:UpdateTable",
                "glue:UpdateUserDefinedFunction"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Sid": "ReadAccessForSecrets",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecrets"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```

## Required Role
