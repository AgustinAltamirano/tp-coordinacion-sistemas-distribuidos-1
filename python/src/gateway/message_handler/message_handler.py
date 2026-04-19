import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.__client_id = str(uuid.uuid4())
        self.__message_count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.__message_count += 1
        return message_protocol.internal.serialize([self.__client_id, fruit, amount])

    def serialize_eof_message(self, message):
        serialized_message = message_protocol.internal.serialize(
            [self.__client_id, self.__message_count]
        )
        self.__message_count = 0
        return serialized_message

    def deserialize_result_message(self, message):
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        if client_id != self.__client_id:
            return None
        return fruit_top
