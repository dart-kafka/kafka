part of kafka.common;

/// Used to indicate there is a mismatch in CRC sum of a message (message is
/// corrupted).
class MessageCrcMismatchError extends StateError {
  MessageCrcMismatchError(String message) : super(message);
}

/// Represents error returned by Kafka server.
class KafkaServerError {
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

  /// Numeric code of this server error.
  final int code;

  static final Map<int, KafkaServerError> _instances = new Map();

  static const Map<int, String> _errorTexts = const {
    0: 'NoError',
    -1: 'Unknown',
    1: 'OffsetOutOfRange',
    2: 'InvalidMessage',
    3: 'UnknownTopicOrPartition',
    4: 'InvalidMessageSize',
    5: 'LeaderNotAvailable',
    6: 'NotLeaderForPartition',
    7: 'RequestTimedOut',
    8: 'BrokerNotAvailable',
    9: 'ReplicaNotAvailable',
    10: 'MessageSizeTooLarge',
    11: 'StaleControllerEpoch',
    12: 'OffsetMetadataTooLarge',
    14: 'OffsetsLoadInProgress',
    15: 'ConsumerCoordinatorNotAvailable',
    16: 'NotCoordinatorForConsumer',
  };

  /// String representation of this server error.
  String get message => _errorTexts[code];

  KafkaServerError._(this.code);

  /// Creates instance of KafkaServerError from numeric error code.
  factory KafkaServerError(int code) {
    if (!_instances.containsKey(code)) {
      _instances[code] = new KafkaServerError._(code);
    }

    return _instances[code];
  }

  @override
  String toString() => 'KafkaServerError: ${message}(${code})';

  bool get isError => code != NoError;
  bool get isNoError => code == NoError;
  bool get isUnknown => code == Unknown;
  bool get isOffsetOutOfRange => code == OffsetOutOfRange;
  bool get isInvalidMessage => code == InvalidMessage;
  bool get isUnknownTopicOrPartition => code == UnknownTopicOrPartition;
  bool get isInvalidMessageSize => code == InvalidMessageSize;
  bool get isLeaderNotAvailable => code == LeaderNotAvailable;
  bool get isNotLeaderForPartition => code == NotLeaderForPartition;
  bool get isRequestTimedOut => code == RequestTimedOut;
  bool get isBrokerNotAvailable => code == BrokerNotAvailable;
  bool get isReplicaNotAvailable => code == ReplicaNotAvailable;
  bool get isMessageSizeTooLarge => code == MessageSizeTooLarge;
  bool get isStaleControllerEpoch => code == StaleControllerEpoch;
  bool get isOffsetMetadataTooLarge => code == OffsetMetadataTooLarge;
  bool get isOffsetsLoadInProgress => code == OffsetsLoadInProgress;
  bool get isConsumerCoordinatorNotAvailable =>
      code == ConsumerCoordinatorNotAvailable;
  bool get isNotCoordinatorForConsumer => code == NotCoordinatorForConsumer;
}
