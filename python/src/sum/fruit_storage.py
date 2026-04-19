import threading

from common import fruit_item


class FruitStorage:
    def __init__(self):
        self.__amount_by_fruit_by_client: dict[str, dict[str, fruit_item.FruitItem]] = (
            {}
        )
        self.__lock = threading.Lock()

    def add_fruit_to_client(self, client_id, fruit, amount):
        with self.__lock:
            current_fruit_item = self.__amount_by_fruit_by_client.setdefault(
                client_id, {}
            ).setdefault(fruit, fruit_item.FruitItem(fruit, 0))
            self.__amount_by_fruit_by_client[client_id][fruit] = (
                current_fruit_item + fruit_item.FruitItem(fruit, int(amount))
            )

    def pop_client_fruits(self, client_id):
        with self.__lock:
            return self.__amount_by_fruit_by_client.pop(client_id, {}).values()
