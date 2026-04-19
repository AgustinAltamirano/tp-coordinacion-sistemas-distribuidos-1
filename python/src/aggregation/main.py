import os
import logging
import heapq

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_fruits = self.fruits_by_client.setdefault(client_id, {})
        client_fruits[fruit] = client_fruits.setdefault(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        client_fruits = self.fruits_by_client.get(client_id, {})
        top_k = heapq.nlargest(TOP_SIZE, client_fruits.values())
        fruit_top = [(item.fruit, item.amount) for item in top_k]
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, fruit_top])
        )
        self.fruits_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
