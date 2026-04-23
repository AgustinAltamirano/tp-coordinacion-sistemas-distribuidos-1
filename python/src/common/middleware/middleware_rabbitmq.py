import pika
from pika.exceptions import AMQPConnectionError
from . import middleware as mid


class MessageMiddlewareRabbitMQ:
    """Base class for RabbitMQ-based message middlewares.

    Establishes the connection and channel with the broker. Subclasses
    must complete initialization by declaring their queues or exchanges.
    """

    def __init__(self, host):
        """Creates a connection and channel with the RabbitMQ broker.

        Args:
            host: RabbitMQ broker address.

        Raises:
            MessageMiddlewareDisconnectedError: If unable to connect to the broker.
            MessageMiddlewareMessageError: If channel setup fails. In this case,
                the connection and channel are closed before raising the exception.
        """
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._setup_channel()

    @staticmethod
    def _handle_exceptions(func):
        """Decorator that translates general exceptions into middleware exceptions.

        Converts AMQPConnectionError into MessageMiddlewareDisconnectedError and
        any other exception into MessageMiddlewareMessageError. On non-connection
        errors, closes the channel and connection before raising.
        """

        def handle_exceptions(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except AMQPConnectionError:
                raise mid.MessageMiddlewareDisconnectedError
            except Exception:
                self.close()
                raise mid.MessageMiddlewareMessageError

        return handle_exceptions

    @_handle_exceptions
    def _setup_channel(self):
        """Creates and configures the channel on the existing connection.
        If it fails, closes the connection before propagating the exception.
        """
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()

    def _generate_on_message_callback(self, on_message_callback):
        """Adapts on_message_callback to the format expected by pika.

        Args:
            on_message_callback: Function with signature (body, ack, nack).

        Returns:
            A pika basic_consume compatible callback.
        """

        def callback(channel, method, _, body):
            def ack_message():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack_message():
                channel.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack_message, nack_message)

        return callback

    @_handle_exceptions
    def _start_consuming(self, queue_name, on_message_callback):
        """Registers the callback and starts consuming from the given queue.

        Args:
            queue_name: Name of the queue to consume from.
            on_message_callback: Function with signature (body, ack, nack).

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=self._generate_on_message_callback(on_message_callback),
        )
        self._channel.start_consuming()

    @_handle_exceptions
    def stop_consuming(self):
        """Stops consuming messages. Has no effect if not currently consuming.

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        self._channel.stop_consuming()

    def request_stop_consuming(self):
        """Thread-safe. Schedules stop_consuming on the connection's event loop."""
        self._connection.add_callback_threadsafe(self._channel.stop_consuming)

    def close(self):
        """Closes the channel and connection with the broker.

        Raises:
            MessageMiddlewareCloseError: If an error occurs while closing.
        """
        try:
            self._channel.close()
            self._connection.close()
        except:
            raise mid.MessageMiddlewareCloseError


class MessageMiddlewareQueueRabbitMQ(
    MessageMiddlewareRabbitMQ, mid.MessageMiddlewareQueue
):
    """Queue-based RabbitMQ message middleware."""

    def __init__(self, host, queue_name):
        """Connects to the broker and declares a durable queue.

        Args:
            host: RabbitMQ broker address.
            queue_name: Name of the queue to declare.

        Raises:
            MessageMiddlewareDisconnectedError: If unable to connect to the broker.
            MessageMiddlewareMessageError: If queue declaration fails. In this case,
                the connection and channel are closed before raising the exception.
        """
        super().__init__(host)
        self.__queue_name = queue_name
        self._setup_queue(queue_name)

    @MessageMiddlewareRabbitMQ._handle_exceptions
    def _setup_queue(self, queue_name):
        """Declares the queue as durable on the broker.

        If it fails, closes the channel and connection before propagating the exception.

        Args:
            queue_name: Name of the queue to declare.
        """
        self._channel.queue_declare(queue=queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        """Starts consuming messages from the queue.

        Invokes on_message_callback(body, ack, nack) for each received message.

        Args:
            on_message_callback: Function with signature (body, ack, nack).

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        self._start_consuming(self.__queue_name, on_message_callback)

    @MessageMiddlewareRabbitMQ._handle_exceptions
    def send(self, message):
        """Sends a message to the queue.

        Args:
            message: Message content to send.

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        self._channel.basic_publish(
            exchange="", routing_key=self.__queue_name, body=message
        )


class MessageMiddlewareExchangeRabbitMQ(
    MessageMiddlewareRabbitMQ, mid.MessageMiddlewareExchange
):
    """Direct exchange-based RabbitMQ message middleware."""

    def __init__(self, host, exchange_name, routing_keys):
        """Connects to the broker and declares a direct exchange.

        Args:
            host: RabbitMQ broker address.
            exchange_name: Name of the exchange to declare.
            routing_keys: List of routing keys for binding.

        Raises:
            MessageMiddlewareDisconnectedError: If unable to connect to the broker.
            MessageMiddlewareMessageError: If exchange declaration fails. In this case,
                the connection and channel are closed before raising the exception.
        """
        super().__init__(host)
        self.__exchange_name = exchange_name
        self.__routing_keys = routing_keys
        self._setup_exchange(exchange_name)

    @MessageMiddlewareRabbitMQ._handle_exceptions
    def _setup_exchange(self, exchange_name):
        """Declares the direct exchange on the broker.

        If it fails, closes the channel and connection before propagating the exception.

        Args:
            exchange_name: Name of the exchange to declare.
        """
        self._channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    @MessageMiddlewareRabbitMQ._handle_exceptions
    def start_consuming(self, on_message_callback):
        """Starts consuming messages from the exchange.

        Declares a temporary exclusive queue, binds it to the exchange with each
        routing key, and starts consuming. Invokes
        on_message_callback(body, ack, nack) for each received message.

        Args:
            on_message_callback: Function with signature (body, ack, nack).

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        result = self._channel.queue_declare(queue="", exclusive=True)
        for routing_key in self.__routing_keys:
            self._channel.queue_bind(
                exchange=self.__exchange_name,
                queue=result.method.queue,
                routing_key=routing_key,
            )
        self._start_consuming(result.method.queue, on_message_callback)

    @MessageMiddlewareRabbitMQ._handle_exceptions
    def send(self, message):
        """Sends a message to the exchange with each configured routing key.

        Args:
            message: Message content to send.

        Raises:
            MessageMiddlewareDisconnectedError: If the connection is lost.
            MessageMiddlewareMessageError: If an internal error occurs.
        """
        for routing_key in self.__routing_keys:
            self._channel.basic_publish(
                exchange=self.__exchange_name,
                routing_key=routing_key,
                body=message,
            )
