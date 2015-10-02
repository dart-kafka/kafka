part of kafka;

/// Base class for all client-side errors.
class KafkaClientError {}

/// Used to indicate there is mismatch in correlation Id between request and response.
class CorrelationIdMismatchError extends KafkaClientError {}

/// Used to indicate there is mismatch in CRC sum of a message (message corrupted).
class MessageCrcMismatchError extends KafkaClientError {
  final String message;
  MessageCrcMismatchError(this.message);
}
