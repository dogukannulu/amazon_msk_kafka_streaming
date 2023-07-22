import csv
import time
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("kafka_topic_create_produce")


class GlobalVariables:
    kafka_broker = 'your_kafka_broker_address:port'
    kafka_topic = 'your_kafka_topic'
    properties_file_path = 'path_to_your_client_properties_file.properties'
    csv_file_path = 'path_to_your_csv_file.csv'


def read_csv_line_by_line(file_path=GlobalVariables.csv_file_path):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            yield ','.join(row)


def create_kafka_topic():
    broker = GlobalVariables.kafka_broker
    topic = GlobalVariables.kafka_topic
    file_path = GlobalVariables.properties_file_path
    admin_client = KafkaAdminClient(
        bootstrap_servers=broker,
        security_protocol='SASL_SSL',
        sasl_mechanism='AWS_MSK_IAM',
        sasl_jaas_config='software.amazon.msk.auth.iam.IAMLoginModule required;',
        sasl_client_callback_handler='software.amazon.msk.auth.iam.IAMClientCallbackHandler',
        ssl_check_hostname=True,
        ssl_cafile=file_path,
    )

    topic = NewTopic(topic, num_partitions=1, replication_factor=3)
    admin_client.create_topics(new_topics=[topic], validate_only=False)


def kafka_topic_producer():
    broker = GlobalVariables.kafka_broker
    file_path = GlobalVariables.properties_file_path
    topic = GlobalVariables.kafka_topic

    producer = KafkaProducer(
        bootstrap_servers=broker,
        security_protocol='SASL_SSL',
        sasl_mechanism='AWS_MSK_IAM',
        sasl_jaas_config='software.amazon.msk.auth.iam.IAMLoginModule required;',
        sasl_client_callback_handler='software.amazon.msk.auth.iam.IAMClientCallbackHandler',
        ssl_check_hostname=True,
        ssl_cafile=file_path,
    )

    try:
        for line in read_csv_line_by_line():
            producer.send(topic, value=line.encode('utf-8'))
            print(f"Sent: {line}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("User interrupted. Closing the producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    create_kafka_topic()
