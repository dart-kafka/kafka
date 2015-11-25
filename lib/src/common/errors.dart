part of kafka.common;

/// Used to indicate there is a mismatch in CRC sum of a message (message is
/// corrupted).
class MessageCrcMismatchError extends StateError {
  MessageCrcMismatchError(String message) : super(message);
}

/// List of error codes returned by Kafka server.
class KafkaServerErrorCode {
  static const int NoError = 0;
  static const int Unknown = -1;
  static const int OffsetOutOfRange = 1;
  static const int InvalidMessage = 2;
  static const int UnknownTopicOrPartition = 3;
  static const int InvalidMessageSize = 4;
  static const int LeaderNotAvailable = 5;
  static const int NotLeaderForPartition = 6;
  static const int RequestTimedOut = 7;
  static const int BrokerNotAvailable = 8;
  static const int ReplicaNotAvailable = 9;
  static const int MessageSizeTooLarge = 10;
  static const int StaleControllerEpoch = 11;
  static const int OffsetMetadataTooLarge = 12;
  static const int OffsetsLoadInProgress = 14;
  static const int ConsumerCoordinatorNotAvailable = 15;
  static const int NotCoordinatorForConsumer = 16;
}

class KafkaServerError {
  final int code;

  static final Map<int, KafkaServerError> _instances = new Map();

  KafkaServerError._(this.code);

  factory KafkaServerError(int code) {
    if (!_instances.containsKey(code)) {
      _instances[code] = new KafkaServerError._(code);
    }

    return _instances[code];
  }

  bool get isError => code != 0;
  bool get isNoError => code == 0;
  bool get isUnknown => code == -1;
  bool get isOffsetOutOfRange => code == 1;
  bool get isInvalidMessage => code == 2;
  bool get isUnknownTopicOrPartition => code == 3;
  bool get isInvalidMessageSize => code == 4;
  bool get isLeaderNotAvailable => code == 5;
  bool get isNotLeaderForPartition => code == 6;
  bool get isRequestTimedOut => code == 7;
  bool get isBrokerNotAvailable => code == 8;
  bool get isReplicaNotAvailable => code == 9;
  bool get isMessageSizeTooLarge => code == 10;
  bool get isStaleControllerEpoch => code == 11;
  bool get isOffsetMetadataTooLarge => code == 12;
  bool get isOffsetsLoadInProgress => code == 14;
  bool get isConsumerCoordinatorNotAvailable => code == 15;
  bool get isNotCoordinatorForConsumer => code == 16;
}
