import threading


class MessageCountController:
    def __init__(self):
        # These two dictionaries are only accessed by the control thread,
        # so they don't need to be protected by a lock.
        self.__global_message_count: dict[str, dict[str, int]] = {}
        self.__global_expected_message_count: dict[str, int] = {}

        # These two data structures are accessed by both the control thread and
        # the data processing threads, so they need to be protected by a lock.
        self.__instance_message_count_by_client: dict[str, int] = {}
        self.__clients_with_eof = set()
        self.__lock = threading.Lock()

    def increase_instance_message_count(self, client_id):
        with self.__lock:
            self.__instance_message_count_by_client[client_id] = (
                self.__instance_message_count_by_client.get(client_id, 0) + 1
            )
            return (
                self.__instance_message_count_by_client[client_id],
                client_id in self.__clients_with_eof,
            )

    def set_global_expected_message_count(self, client_id, expected_message_count):
        self.__global_expected_message_count[client_id] = expected_message_count
        with self.__lock:
            self.__clients_with_eof.add(client_id)
            return self.__instance_message_count_by_client.get(client_id, 0)

    def has_client_eof(self, client_id):
        with self.__lock:
            return client_id in self.__clients_with_eof

    def update_global_message_count(self, client_id, sum_instance_id, message_count):
        client_message_count_by_instance = self.__global_message_count.setdefault(
            client_id, {}
        )
        client_message_count_by_instance[sum_instance_id] = max(
            client_message_count_by_instance.get(sum_instance_id, 0), message_count
        )

    def client_has_received_all_messages(self, client_id):
        expected_message_count = self.__global_expected_message_count.get(client_id)
        if expected_message_count is None:
            return False
        global_client_message_count = self.__global_message_count.get(client_id, {})
        total_message_count = sum(global_client_message_count.values())
        return total_message_count >= expected_message_count

    def reset_client_count(self, client_id):
        self.__global_message_count.pop(client_id, None)
        self.__global_expected_message_count.pop(client_id, None)
        with self.__lock:
            self.__instance_message_count_by_client.pop(client_id, None)
            self.__clients_with_eof.discard(client_id)
