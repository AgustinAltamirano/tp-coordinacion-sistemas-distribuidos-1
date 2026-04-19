import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item
from .control_message_constants import ControlMessageType
from .fruit_storage import FruitStorage
from .message_count_controller import MessageCountController

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_exchange_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        self.data_output_queues = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
            )
            self.data_output_queues.append(data_output_queue)
        self.fruit_storage = FruitStorage()
        self.message_count_controller = MessageCountController()
        self.control_thread = None
        self.control_exchange_control = None
        self.sigterm_received = threading.Event()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.fruit_storage.add_fruit_to_client(client_id, fruit, int(amount))
        message_count, eof_received = (
            self.message_count_controller.increase_instance_message_count(client_id)
        )
        if eof_received:
            self.control_exchange_output.send(
                message_protocol.internal.serialize(
                    [
                        ControlMessageType.PROCESSED_MESSAGE_COUNT.value,
                        client_id,
                        ID,
                        message_count,
                    ]
                )
            )

    def _process_eof(self, client_id, message_count):
        logging.info(f"Received EOF from input queue")
        self.control_exchange_output.send(
            message_protocol.internal.serialize(
                [
                    ControlMessageType.EOF_RECEIVED.value,
                    client_id,
                    message_count,
                ]
            )
        )

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _process_control_eof_received(self, client_id, message_count):
        instance_processed_message_count = (
            self.message_count_controller.set_global_expected_message_count(
                client_id, message_count
            )
        )
        if not self.control_exchange_control:
            logging.error("Control exchange not initialized")
            return

        self.control_exchange_control.send(
            message_protocol.internal.serialize(
                [
                    ControlMessageType.PROCESSED_MESSAGE_COUNT.value,
                    client_id,
                    ID,
                    instance_processed_message_count,
                ]
            )
        )

    def _process_control_processed_message_count(
        self, client_id, sum_instance_id, message_count
    ):
        if not self.message_count_controller.has_client_eof(client_id):
            return
        self.message_count_controller.update_global_message_count(
            client_id, sum_instance_id, message_count
        )
        if self.message_count_controller.client_has_received_all_messages(client_id):
            self._flush_client_fruits(client_id)
            self.message_count_controller.reset_client_count(client_id)

    def _flush_client_fruits(self, client_id):
        logging.info(f"Flushing fruits for client {client_id}")
        for final_fruit_item in self.fruit_storage.pop_client_fruits(client_id):
            destination_index = self._client_fruit_hash(client_id, final_fruit_item)
            self.data_output_queues[destination_index].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        for data_output_queue in self.data_output_queues:
            data_output_queue.send(message_protocol.internal.serialize([client_id]))

    def _client_fruit_hash(self, client_id: str, fruit: fruit_item.FruitItem) -> int:
        return (
            zlib.crc32(f"{client_id}_{fruit.fruit}".encode("utf-8"))
            % AGGREGATION_AMOUNT
        )

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == ControlMessageType.EOF_RECEIVED.value:
            self._process_control_eof_received(fields[1], fields[2])
        elif fields[0] == ControlMessageType.PROCESSED_MESSAGE_COUNT.value:
            self._process_control_processed_message_count(
                fields[1], fields[2], fields[3]
            )
        ack()

    def start_control(self):
        self.control_exchange_control = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        if self.sigterm_received.is_set():
            return
        self.control_exchange_control.start_consuming(self.process_control_message)

    def handle_sigterm(self):
        logging.info("SIGTERM received, requesting shutdown")
        self.sigterm_received.set()
        try:
            self.input_queue.request_stop_consuming()
        except Exception as e:
            logging.error(e)
        if self.control_exchange_control is not None:
            try:
                self.control_exchange_control.request_stop_consuming()
            except Exception as e:
                logging.error(e)

    def _close_resources(self):
        for data_output_queue in self.data_output_queues:
            try:
                data_output_queue.close()
            except Exception as e:
                logging.error(e)
        for middleware in (
            self.input_queue,
            self.control_exchange_output,
            self.control_exchange_control,
        ):
            if middleware is None:
                continue
            try:
                middleware.close()
            except Exception as e:
                logging.error(e)

    def start(self):
        self.control_thread = threading.Thread(target=self.start_control)
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_message)
        finally:
            if self.control_exchange_control is not None:
                try:
                    self.control_exchange_control.request_stop_consuming()
                except Exception as e:
                    logging.error(e)
            self.control_thread.join()
            self._close_resources()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: sum_filter.handle_sigterm(),
    )
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
