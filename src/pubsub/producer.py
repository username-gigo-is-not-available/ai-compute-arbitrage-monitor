import json
import logging
from confluent_kafka import Producer, KafkaException

from config.kafka import KafkaConfig



class KafkaProducer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self._producer = Producer(self._build_config())

    def _build_config(self):
        bootstrap_servers: str = self.config.bootstrap_servers
        api_key: str = self.config.api_key
        api_secret: str = self.config.api_secret
        cfg = {"bootstrap.servers": bootstrap_servers}

        if api_key and api_secret:
            cfg.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": api_key,
                "sasl.password": api_secret,
            })

        return cfg

    def produce(self, topic: str, payload: dict, key: str | None = None) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=self._delivery_report,
            )
            self._producer.poll(0)  # trigger delivery callbacks
        except KafkaException as e:
            logging.error(f"Failed to produce message to {topic}: {e}")

    def flush(self) -> None:
        self._producer.flush()
        logging.debug("Kafka producer flushed.")

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


