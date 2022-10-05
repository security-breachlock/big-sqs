# BigSQS
An SQS client capable of storing oversize message payloads on S3.

## Overview
AWS SQS is a super useful message queue, but it's sometimes the case that we want to transmit messages larger than the 256KB limit. An official [SQS extended client library](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) is available for Java, but not for Python. Similar libraries implementing the protocol used by the original Java library are available for Python, but this library has a few additional features:

* **Fully transparent response structure** - MD5 hashes (`MD5OfBody`) and the `content-length` header are recomputed clent-side to be correct to the message *after* resolution of S3 pointers.
* **Unopinionated configuration** - The library can use your default (environment) AWS creds (useful for deployment in Lambda functions), take your AWS creds as paremeters and even supports using 2 different credential sets for SQS and S3, even if these belong to different AWS accounts.
* **Leaves boto3 untouched** - This library does not attempt to reconfigure/decorate boto3 with additional functionality.
* **Fully documented** - The library is fully documented with docstrings, making for an enjoyable development experience.

## Installing
Installing the project is very straightforward via [pip](https://pypi.org/project/pip/):

```bash
pip install big-sqs
```

You can then import the library into your project:

```python
from big_sqs import BigSqsClient
```

## Building
Building the library is only necessary if you're doing development work on it, and is not required if you're just importing it to use in your project. To build the library, you'll need to install the Poetry dependency management system for Python. Build the project like so:

```bash
poetry build
```

## Usage
Use the library like so:

```python
from big_sqs import BigSqsClient

# Initialize client.
sqs = BigSqsClient.from_default_aws_creds(
    '<my_queue_url>',
    '<my_s3_bucket_name>',
    1024, # For any messages bigger than 1KiB, use S3.
)

# Create 2KiB message.
PAYLOAD_SIZE = 2048
payload = '0' * PAYLOAD_SIZE

# Send message.
sqs.send_message(payload)

# Receive that same message.
dequeued = sqs.receive_messages(1)

# Print the message payload we got back.
print(dequeued)

# Delete messages (S3 objects will also be cleaned up).
for message in recv['Messages']:
    sqs.delete_message(message['ReceiptHandle'])
```

## Configuration
You can configure the library with your SQS credentials in 3 ways:

### Using Default (Environment) Creds
To use the default AWS credentials configured for your environment (if any) you can use the `from_default_aws_creds` static factory method:

```python
from big_sqs import BigSqsClient

# Initialize client.
sqs = BigSqsClient.from_default_aws_creds(
    '<my_queue_url>',
    '<my_s3_bucket_name>',
    1024, # For any messages bigger than 1KiB, use S3.
)
```

### User-Specified Creds
To make use of user-specified AWS credentials, there's a different factory method for you to use:

```python
from big_sqs import BigSqsClient

# Initialize client.
sqs = BigSqsClient.from_aws_creds(
    'us-west-2',
    '<my_aws_access_key_id>',
    '<my_aws_secret_access_key>',
    '<my_queue_url>',
    '<my_s3_bucket_name>',
    1024, # For any messages bigger than 1KiB, use S3.
)
```

### User-Specified Clients
To use a different set of credentials for SQS and S3, or to use different AWS accounts for each, you can supply boto3 clients directly to the `BigSqsClient` constructor.

```python
from big_sqs import BigSqsClient

# Initialize client.
sqs = BigSqsClient(
    boto3.client(
        'sqs',
        region_name='us-west-2',
        aws_access_key_id='<my_us_aws_access_key_id>',
        aws_secret_access_key='<my_us_aws_secret_access_key>',
    ),
    boto3.client(
        's3',
        region_name='eu-west-2',
        aws_access_key_id='<my_eu_aws_access_key_id>',
        aws_secret_access_key='<my_eu_aws_secret_access_key>',
    ),
    '<my_queue_url>',
    '<my_s3_bucket_name>',
    1024, # For any messages bigger than 1KiB, use S3.
)
```

## Acknowledgements
The authors acknowledge the contribution of the following projects to this library.

* The original [SQS extended client library](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) (for Java)

## Contributors
The main contributors to this project so far are as follows:

* Saul Johnson ([@lambdacasserole](https://github.com/lambdacasserole))
