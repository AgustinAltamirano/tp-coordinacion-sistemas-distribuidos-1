import heapq
import logging
import os
import signal
import time

from common import middleware, message_protocol, fruit_item
from common.middleware.middleware import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

RETRY_DELAYS = (1, 2, 4, 8, 16)


class JoinFilter:

    def __init__(self):
        self.input_queue = None
        self.output_queue = None
        self.fruits_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}
        self.partial_tops_received_by_client: dict[str, int] = {}
        try:
            self._build_middlewares()
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            self._close_resources()
            raise

    def _build_middlewares(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        client_id, partial_fruit_top = message_protocol.internal.deserialize(message)
        self._add_partial_top(client_id, partial_fruit_top)
        self.partial_tops_received_by_client[client_id] = (
            self.partial_tops_received_by_client.get(client_id, 0) + 1
        )
        logging.info(
            f"Received top {self.partial_tops_received_by_client[client_id]}"
            f"/{AGGREGATION_AMOUNT} for client {client_id}"
        )
        if self.partial_tops_received_by_client[client_id] < AGGREGATION_AMOUNT:
            ack()
            return
        final_fruit_top = self._calculate_final_fruit_top(client_id)
        logging.info(f"Emitting final top for client {client_id}")
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
        assert self.output_queue is not None
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_fruit_top])
        )

    def handle_sigterm(self):
        logging.info("SIGTERM received, requesting shutdown")
        if self.input_queue is not None:
            try:
                self.input_queue.request_stop_consuming()
            except Exception as e:
                logging.error(e)

    def _close_resources(self):
        for mw in (self.input_queue, self.output_queue):
            if mw is None:
                continue
            try:
                mw.close()
            except MessageMiddlewareCloseError as close_err:
                logging.error(close_err)
        self.input_queue = None
        self.output_queue = None

    def _run(self):
        assert self.input_queue is not None
        self.input_queue.start_consuming(self.process_messsage)

    def start(self):
        attempt = 0
        while True:
            try:
                self._run()
                self._close_resources()
                return
            except MessageMiddlewareMessageError as message_err:
                logging.error(f"MessageError: {message_err}")
                self._close_resources()
                raise
            except MessageMiddlewareDisconnectedError:
                if attempt >= len(RETRY_DELAYS):
                    logging.error("Disconnected: retries exhausted")
                    self._close_resources()
                    raise
                delay = RETRY_DELAYS[attempt]
                logging.warning(
                    f"Disconnected, retry {attempt + 1}/{len(RETRY_DELAYS)} "
                    f"in {delay}s"
                )
                self._close_resources()
                time.sleep(delay)
                attempt += 1
                try:
                    self._build_middlewares()
                except MessageMiddlewareMessageError as message_err_2:
                    logging.error(f"MessageError during rebuild: {message_err_2}")
                    self._close_resources()
                    raise
                except MessageMiddlewareDisconnectedError as message_err_2:
                    logging.warning(
                        f"Rebuild failed due to Disconnected: {message_err_2}"
                    )
                    continue


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: join_filter.handle_sigterm(),
    )
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
