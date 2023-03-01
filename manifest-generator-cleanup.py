import boto3
import json
import time

sqs = boto3.resource("sqs", region_name="us-west-2")
queue = sqs.get_queue_by_name(QueueName="manifest-generator")
sqs_client = boto3.client("sqs", "us-west-2")


def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
        )

        try:
            yield from resp["Messages"]
        except KeyError:
            return


def lambda_handler(event, context):
    messages = get_messages_from_queue(
        "https://sqs.us-west-2.amazonaws.com/843093192195/manifest-generator"
    )
    for message in messages:
        sqs_receive_timestamp = int(
            message["Attributes"]["ApproximateFirstReceiveTimestamp"]
        )
        sqs_body = json.loads(message["Body"])
        sqs_bcid = sqs_body["bcid"]
        time_of_new_message = time.time() * 1000
        for comp_message in messages:
            comp_sqs_receive_timestamp = int(
                comp_message["Attributes"]["ApproximateFirstReceiveTimestamp"]
            )
            comp_sqs_body = json.loads(comp_message["Body"])
            comp_sqs_bcid = sqs_body["bcid"]
            comp_time_of_new_message = time.time() * 1000

            if (
                sqs_bcid == comp_sqs_bcid
                and time_of_new_message - sqs_receive_timestamp > 30000
            ):
                entries = [
                    {
                        "Id": message["MessageId"],
                        "ReceiptHandle": message["ReceiptHandle"],
                    }
                ]
                sqs_client.delete_message_batch(
                    QueueUrl="https://sqs.us-west-2.amazonaws.com/843093192195/manifest-generator",
                    Entries=entries,
                )


if __name__ == "__main__":
    event = {
        "start": "2023-02-16T15:47:06-07:00",
        "end": "2023-02-16T15:50:36-07:00",
        "manifest_filter": "",
        "hls_manifest": "https://media.psdcdn.churchofjesuschrist.org/dvr-hls-027.m3u8",
        "bcid": "test-3-dvr-captions-g1",
        "audio_languages": "eng,spa,por,fra,deu,ita,rus,jpn,yue,cmn,kor,ceb,tgl,smo,ton",
        "subtitle_languages": "eng,spa,por",
        "audioOnlyManifest": "False",
        "env": "",
    }
    lambda_handler(event, "test")
