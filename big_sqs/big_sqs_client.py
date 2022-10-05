""" Contains an SQS client capable of storing oversize message payloads on S3.

Author:
    Saul Johnson (saul.johnson@breachlock.com)
Since:
    05/10/2022
"""

import json
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import hashlib

import boto3


class BigSqsClient():
    """ Represents an SQS client capable of storing oversize message payloads on S3.
    """


    MAX_SQS_MESSAGE_SIZE = 262144
    """ The maximum message size supported by SQS in bytes.
    """


    def __init__(
        self,
        sqs: Any,
        s3: Any,
        queue_url: str,
        bucket_name: str,
        size_threshold: int=MAX_SQS_MESSAGE_SIZE):
        """ Initializes a new SQS client capable of storing oversize message payloads on S3.

        Args:
            sqs (botocore.client.SQS): The SQS client to use.
            s3 (botocore.client.S3): The S3 client to use.
            queue_url (str): The URL of the queue to connect to.
            bucket_name (str): The name of the S3 bucket to use to store overside message payloads.
            size_threshold (int): The size limit (in bytes) above which S3 should be used to store message payloads.
        """
        self._sqs = sqs
        self._s3 = s3
        self._queue_url = queue_url
        self._bucket_name = bucket_name
        self._size_threshold = size_threshold
        self._receipt_handle_lookup = {}


    @staticmethod
    def utf8len(message: str) -> int:
        """ Gets the length of a string in bytes, when encoded as UTF-8.

        Args:
            message (str): The string to check the length of, in bytes.
        Returns:
            int: The length of the string in bytes when encoded as UTF-8.
        """
        return len(message.encode('utf-8'))


    def send_message(
        self,
        message: str,
        attributes: Dict[str, Any] = None,
        message_group_id: Optional[str] = None) -> Dict[str, Any]:
        """ Sends an SQS message, substituting an S3 pointer for oversize payloads if necessary.

        Args:
            message (str): The message to send.
            attributes (Dict[str, Any]): Any attributes to attach to the message.
            message_group_id (Optional[str]): The message group ID to attach to the message (defaults to a new UUID).
        Returns:
            Dict[str, Any]: The response from SQS.
        """

        # Create a unique ID. Start off by assuming our message is the SQS payload.
        payload_id = str(uuid4())
        payload = message

        # If we exceed the size threshold, we'll need to make use of S3.
        if BigSqsClient.utf8len(message) > self._size_threshold:

            # Upload to S3 and substitute out payload for pointer.
            self._s3.put_object(
                Body=message,
                Bucket=self._bucket_name,
                Key=payload_id,
                ContentType='text/plain',
            )
            payload = json.dumps([
                'com.amazon.sqs.javamessaging.MessageS3Pointer',
                {
                    's3BucketName': self._bucket_name,
                    's3Key': payload_id,
                },
            ])

        # Finally send message to SQS.
        return self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageDeduplicationId=payload_id,
            MessageGroupId=message_group_id if message_group_id is not None else str(uuid4()),
            MessageAttributes=attributes if attributes is not None else {},
            MessageBody=payload
        )


    def send_messages(
        self,
        messages: List[Tuple[str, Dict[str, Any], str]]) -> List[Dict[str, Any]]:
        """ Sends multiple SQS messages, substituting an S3 pointer for oversize payloads if necessary.

        Args:
            messages (List[Tuple[str, Dict[str, Any], str]]): The messages to send as (payload, attrs, id) tuples.
        Returns:
            List[Dict[str, Any]]: The responses from SQS.
        """
        return list(map(lambda message: self.send_message(*message), messages))


    @staticmethod
    def is_s3_pointer(message: Dict[str, Any]) -> bool:
        """ Gets whether or not the given SQS message is an S3 pointer.

        Args:
            message (Dict[str, Any]): The message to check.
        Returns:
            bool: True if the message in an S3 pointer, otherwise False.
        """

        # Capture message body. We need to determine if this represents an S3 pointer.
        body = message['Body']
        try:

            # Attempt to parse as JSON.
            parsed_body = json.loads(body)

            # We should have a 2-list consisting of a Java fully-qualified type name and S3 pointer.
            return type(parsed_body) is list and len(parsed_body) == 2 \
                and parsed_body[0] == 'com.amazon.sqs.javamessaging.MessageS3Pointer' \
                and type(parsed_body[1]) is dict \
                and 's3BucketName' in parsed_body[1] and 's3Key' in parsed_body[1]
        except json.JSONDecodeError:

            # Can't parse as JSON. If this fails, this is not an S3 pointer.
            return False


    def receive_messages(
        self,
        max_number_of_messages: int,
        attributes: Optional[List[str]]=None) -> Dict[str, Any]:
        """ Receives one or more messages from SQS, resolving any pointers to oversize payloads on S3.

        Args:
            max_number_of_messages (Optional[int]): The maximum number of messages to receive (defaults to 1).
            attributes (Optional[List[str]]): The attributes to return with the message (defaults to all).
        Returns:
            Dict[str, Any]: The response from SQS, with oversize payloads resolved via S3.
        """

        # Query SQS.
        sqs_response = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            MessageAttributeNames=['All'] if attributes is None else attributes,
            WaitTimeSeconds=20,
        )

        # Go through each message in the response and resolve any S3 pointers.
        for message in sqs_response.get('Messages', []):

            # If the message is an S3 pointer, attempt to resolve it.
            if BigSqsClient.is_s3_pointer(message):

                # Destructure pointer.
                body = json.loads(message['Body'])
                s3_bucket_name = body[1]['s3BucketName']
                s3_key = body[1]['s3Key']

                # Record pointer against receipt handle, for when we delete.
                self._receipt_handle_lookup[message['ReceiptHandle']] = {
                    's3_bucket_name': s3_bucket_name,
                    's3_key': s3_key,
                }

                # Pull in oversize payload from S3 and assign in place of SQS message body.
                s3_response = self._s3.get_object(Bucket=s3_bucket_name, Key=s3_key)
                body_bytes = s3_response['Body'].read()
                message['Body'] = body_bytes.decode('utf-8')
                message['MD5OfBody'] = hashlib.md5(body_bytes).hexdigest() # Update MD5 hash.

                # Correct content length.
                content_length = int(sqs_response['ResponseMetadata']['HTTPHeaders']['content-length'])
                new_content_length = len(json.dumps(sqs_response))
                new_content_length += len(str(new_content_length)) - len(str(content_length))
                sqs_response['ResponseMetadata']['HTTPHeaders']['content-length'] = str(new_content_length)

        # Return response with S3 pointers resolved.
        return sqs_response


    def delete_message(self, receipt_handle: str) -> Dict[str, Any]:
        """ Deletes an SQS message from the queue, cleaning up its associated S3 object if necessary.

        Args:
            receipt_handle (str): The receipt handle of the message to delete.
        Returns:
            Dict[str, Any]: The response from SQS.
        """

        # Delete the message on SQS.
        sqs_response = self._sqs.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        # Look up whether or not we have an S3 pointer on record for this receipt handle and delete if so.
        if receipt_handle in self._receipt_handle_lookup:
            s3_pointer = self._receipt_handle_lookup[receipt_handle]
            self._s3.delete_object(
                Bucket=s3_pointer['s3_bucket_name'],
                Key=s3_pointer['s3_key'],
            )
        return sqs_response


    @staticmethod
    def from_aws_creds(
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        queue_url: str,
        bucket_name: str,
        size_threshold: int=MAX_SQS_MESSAGE_SIZE) -> 'BigSqsClient':
        """ Initializes a new SQS client capable of handling large messages, using the given AWS credentials.

        Args:
            region_name (str): The AWS region name.
            aws_access_key_id (str): The AWS access key ID to use.
            aws_secret_access_key (str): The AWS secret access key to use.
            queue_url (str): The URL of the queue to connect to.
            bucket_name (str): The name of the S3 bucket to use to store overside message payloads.
            size_threshold (int): The size limit (in bytes) above which S3 should be used to store message payloads.
        Returns:
            BigSqsClient: The newly-initialized client.
        """
        return BigSqsClient(
            boto3.client(
                'sqs',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            ),
            boto3.client(
                's3',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            ),
            queue_url,
            bucket_name,
            size_threshold,
        )


    @staticmethod
    def from_default_aws_creds(
        queue_url: str,
        bucket_name: str,
        size_threshold: int=MAX_SQS_MESSAGE_SIZE) -> 'BigSqsClient':
        """ Initializes a new SQS client capable of handling large messages, from the default AWS credentials present
        in the environment.

        Args:
            queue_url (str): The URL of the queue to connect to.
            bucket_name (str): The name of the S3 bucket to use to store overside message payloads.
            size_threshold (int): The size limit (in bytes) above which S3 should be used to store message payloads.
        Returns:
            BigSqsClient: The newly-initialized client.
        """
        return BigSqsClient(boto3.client('sqs'), boto3.client('s3'), queue_url, bucket_name, size_threshold)
