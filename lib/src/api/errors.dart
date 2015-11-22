part of kafka.protocol;

/// Used to indicate there is mismatch in CRC sum of a message (message corrupted).
class MessageCrcMismatchError extends StateError {
  MessageCrcMismatchError(String message) : super(message);
}

/// Represents API errors produced by Kafka server.
class KafkaApiError {
  final Map errorCodes = {
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
    11: 'StaleControllerEpochCode',
    12: 'OffsetMetadataTooLargeCode',
    14: 'OffsetsLoadInProgressCode',
    15: 'ConsumerCoordinatorNotAvailableCode',
    16: 'NotCoordinatorForConsumerCode',
  };

  final int errorCode;

  String get errorMessage => errorCodes[errorCode];

  KafkaApiError.fromErrorCode(this.errorCode);

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
