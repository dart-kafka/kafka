part of kafka;

/// Base class for all client-side errors.
class KafkaClientError {
  final String message;
  KafkaClientError(this.message);
}

/// Used to indicate there is mismatch in correlation Id between request and response.
class CorrelationIdMismatchError extends KafkaClientError {
  CorrelationIdMismatchError(String message) : super(message);
}

/// Used to indicate there is mismatch in CRC sum of a message (message corrupted).
class MessageCrcMismatchError extends KafkaClientError {
  MessageCrcMismatchError(String message) : super(message);
}
