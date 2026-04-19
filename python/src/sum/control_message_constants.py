from enum import Enum


class ControlMessageType(Enum):
    EOF_RECEIVED = "EOF_RECEIVED"
    PROCESSED_MESSAGE_COUNT = "PROCESSED_MESSAGE_COUNT"
