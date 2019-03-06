# Preparing the Environment

Since we will read from S3, you need some valid S3 credentials
    AWS_ACCESS_KEY_ID=your_aws_key
    AWS_SECRET_ACCESS_KEY=your_aws_secret
    AWS_PROXY_HOST=
    AWS_PROXY_PORT=

# Using flowman

## Running the whole project

    flowexec -f examples/weather project run

## Executing outputs

    flowexec -f examples/weather target build
