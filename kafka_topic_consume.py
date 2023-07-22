import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("kafka_topic_create_produce")


class GlobalVariables:
    kafka_broker = 'your_kafka_broker_address:port'
    kafka_topic = 'your_kafka_topic'
    properties_file_path = 'path_to_your_client_properties_file.properties'
    csv_file_path = 'path_to_your_csv_file.csv'

def kafka_topic_consumer():
    topic = GlobalVariables.kafka_topic
    broker = GlobalVariables.kafka_broker
    file_path = GlobalVariables.properties_file_path
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        security_protocol='SASL_SSL',
        sasl_mechanism='AWS_MSK_IAM',
        sasl_jaas_config='software.amazon.msk.auth.iam.IAMLoginModule required;',
        sasl_client_callback_handler='software.amazon.msk.auth.iam.IAMClientCallbackHandler',
        ssl_check_hostname=True,
        ssl_cafile=file_path,
    )

    try:
        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')}")
    except KeyboardInterrupt:
        print("User interrupted. Closing the consumer.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
