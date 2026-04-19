import heapq
import logging
import os

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruits_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}
        self.partial_tops_received_by_client: dict[str, int] = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_id, partial_fruit_top = message_protocol.internal.deserialize(message)
        self._add_partial_top(client_id, partial_fruit_top)
        self.partial_tops_received_by_client[client_id] = (
            self.partial_tops_received_by_client.get(client_id, 0) + 1
        )
        if self.partial_tops_received_by_client[client_id] == AGGREGATION_AMOUNT:
            final_fruit_top = self._calculate_final_fruit_top(client_id)
            self._send_final_fruit_top(client_id, final_fruit_top)
            self.fruits_by_client.pop(client_id, None)
            self.partial_tops_received_by_client.pop(client_id, None)
        ack()

    def _add_partial_top(self, client_id, partial_fruit_top):
        client_fruits = self.fruits_by_client.setdefault(client_id, {})
        for fruit, amount in partial_fruit_top:
            client_fruits[fruit] = client_fruits.setdefault(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)

    def _calculate_final_fruit_top(self, client_id):
        client_fruits = self.fruits_by_client.get(client_id, {})
        top_k = heapq.nlargest(TOP_SIZE, client_fruits.values())
        final_fruit_top = [(item.fruit, item.amount) for item in top_k]
        return final_fruit_top

    def _send_final_fruit_top(self, client_id, final_fruit_top):
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_fruit_top])
        )
        pass

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
