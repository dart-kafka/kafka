import 'common.dart';

/// Used to indicate there is a mismatch in CRC sum of a message (message is
/// corrupted).
class MessageCrcMismatchError extends StateError {
  MessageCrcMismatchError(String message) : super(message);
}

/// List of all Kafka server error codes.
abstract class Errors {
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
  static const int InvalidTopic = 17;
  static const int RecordListTooLarge = 18;
  static const int NotEnoughReplicas = 19;
  static const int NotEnoughReplicasAfterAppend = 20;
  static const int InvalidRequiredAcks = 21;
  static const int IllegalGeneration = 22;
  static const int InconsistentGroupProtocol = 23;
  static const int InvalidGroupId = 24;
  static const int UnknownMemberId = 25;
  static const int InvalidSessionTimeout = 26;
  static const int RebalanceInProgress = 27;
  static const int InvalidCommitOffsetSize = 28;
  static const int TopicAuthorizationFailed = 29;
  static const int GroupAuthorizationFailed = 30;
  static const int ClusterAuthorizationFailed = 31;
  static const int InvalidTimestamp = 32;
  static const int UnsupportedSaslMechanism = 33;
  static const int IllegalSaslState = 34;
  static const int UnsupportedVersion = 35;
}

/// Represents errors returned by Kafka server.
class KafkaError {
  /// Numeric code of this server error.
  final int code;

  /// The response object associated containing this error.
  final response;

  KafkaError._(this.code, this.response);

  factory KafkaError.fromCode(int code, response) {
    switch (code) {
      case Errors.Unknown:
        return new UnknownError(response);
      case Errors.OffsetOutOfRange:
        return new OffsetOutOfRangeError(response, null);
      case Errors.InvalidMessage:
        return new InvalidMessageError(response);
      case Errors.UnknownTopicOrPartition:
        return new UnknownTopicOrPartitionError(response);
      case Errors.InvalidMessageSize:
        return new InvalidMessageSizeError(response);
      case Errors.LeaderNotAvailable:
        return new LeaderNotAvailableError(response);
      case Errors.NotLeaderForPartition:
        return new NotLeaderForPartitionError(response);
      case Errors.RequestTimedOut:
        return new RequestTimedOutError(response);
      case Errors.BrokerNotAvailable:
        return new BrokerNotAvailableError(response);
      case Errors.ReplicaNotAvailable:
        return new ReplicaNotAvailableError(response);
      case Errors.MessageSizeTooLarge:
        return new MessageSizeTooLargeError(response);
      case Errors.StaleControllerEpoch:
        return new StaleControllerEpochError(response);
      case Errors.OffsetMetadataTooLarge:
        return new OffsetMetadataTooLargeError(response);
      case Errors.OffsetsLoadInProgress:
        return new OffsetsLoadInProgressError(response);
      case Errors.ConsumerCoordinatorNotAvailable:
        return new ConsumerCoordinatorNotAvailableError(response);
      case Errors.NotCoordinatorForConsumer:
        return new NotCoordinatorForConsumerError(response);
      case Errors.InvalidTopic:
        return new InvalidTopicError(response);
      case Errors.RecordListTooLarge:
        return new RecordListTooLargeError(response);
      case Errors.NotEnoughReplicas:
        return new NotEnoughReplicasError(response);
      case Errors.NotEnoughReplicasAfterAppend:
        return new NotEnoughReplicasAfterAppendError(response);
      case Errors.InvalidRequiredAcks:
        return new InvalidRequiredAcksError(response);
      case Errors.IllegalGeneration:
        return new IllegalGenerationError(response);
      case Errors.InconsistentGroupProtocol:
        return new InconsistentGroupProtocolError(response);
      case Errors.InvalidGroupId:
        return new InvalidGroupIdError(response);
      case Errors.UnknownMemberId:
        return new UnknownMemberIdError(response);
      case Errors.InvalidSessionTimeout:
        return new InvalidSessionTimeoutError(response);
      case Errors.RebalanceInProgress:
        return new RebalanceInProgressError(response);
      case Errors.InvalidCommitOffsetSize:
        return new InvalidCommitOffsetSizeError(response);
      case Errors.TopicAuthorizationFailed:
        return new TopicAuthorizationFailedError(response);
      case Errors.GroupAuthorizationFailed:
        return new GroupAuthorizationFailedError(response);
      case Errors.ClusterAuthorizationFailed:
        return new ClusterAuthorizationFailedError(response);
      case Errors.InvalidTimestamp:
        return new InvalidTimestampError(response);
      case Errors.UnsupportedSaslMechanism:
        return new UnsupportedSaslMechanismError(response);
      case Errors.IllegalSaslState:
        return new IllegalSaslStateError(response);
      case Errors.UnsupportedVersion:
        return new UnsupportedVersionError(response);
      default:
        throw new ArgumentError('Unsupported Kafka server error code $code.');
    }
  }

  @override
  String toString() => '${runtimeType}(${code})';
}

class NoError extends KafkaError {
  NoError(response) : super._(Errors.NoError, response);
}

class UnknownError extends KafkaError {
  UnknownError(response) : super._(Errors.Unknown, response);
}

class OffsetOutOfRangeError extends KafkaError {
  final List<TopicPartition> topicPartitions;
  OffsetOutOfRangeError(response, this.topicPartitions)
      : super._(Errors.OffsetOutOfRange, response);
}

class InvalidMessageError extends KafkaError {
  InvalidMessageError(response) : super._(Errors.InvalidMessage, response);
}

class UnknownTopicOrPartitionError extends KafkaError {
  UnknownTopicOrPartitionError(response)
      : super._(Errors.UnknownTopicOrPartition, response);
}

class InvalidMessageSizeError extends KafkaError {
  InvalidMessageSizeError(response)
      : super._(Errors.InvalidMessageSize, response);
}

class LeaderNotAvailableError extends KafkaError {
  LeaderNotAvailableError(response)
      : super._(Errors.LeaderNotAvailable, response);
}

class NotLeaderForPartitionError extends KafkaError {
  NotLeaderForPartitionError(response)
      : super._(Errors.NotLeaderForPartition, response);
}

class RequestTimedOutError extends KafkaError {
  RequestTimedOutError(response) : super._(Errors.RequestTimedOut, response);
}

class BrokerNotAvailableError extends KafkaError {
  BrokerNotAvailableError(response)
      : super._(Errors.BrokerNotAvailable, response);
}

class ReplicaNotAvailableError extends KafkaError {
  ReplicaNotAvailableError(response)
      : super._(Errors.ReplicaNotAvailable, response);
}

class MessageSizeTooLargeError extends KafkaError {
  MessageSizeTooLargeError(response)
      : super._(Errors.MessageSizeTooLarge, response);
}

class StaleControllerEpochError extends KafkaError {
  StaleControllerEpochError(response)
      : super._(Errors.StaleControllerEpoch, response);
}

class OffsetMetadataTooLargeError extends KafkaError {
  OffsetMetadataTooLargeError(response)
      : super._(Errors.OffsetMetadataTooLarge, response);
}

class OffsetsLoadInProgressError extends KafkaError {
  OffsetsLoadInProgressError(response)
      : super._(Errors.OffsetsLoadInProgress, response);
}

class ConsumerCoordinatorNotAvailableError extends KafkaError {
  ConsumerCoordinatorNotAvailableError(response)
      : super._(Errors.ConsumerCoordinatorNotAvailable, response);
}

class NotCoordinatorForConsumerError extends KafkaError {
  NotCoordinatorForConsumerError(response)
      : super._(Errors.NotCoordinatorForConsumer, response);
}

class InvalidTopicError extends KafkaError {
  InvalidTopicError(response) : super._(Errors.InvalidTopic, response);
}

class RecordListTooLargeError extends KafkaError {
  RecordListTooLargeError(response)
      : super._(Errors.RecordListTooLarge, response);
}

class NotEnoughReplicasError extends KafkaError {
  NotEnoughReplicasError(response)
      : super._(Errors.NotEnoughReplicas, response);
}

class NotEnoughReplicasAfterAppendError extends KafkaError {
  NotEnoughReplicasAfterAppendError(response)
      : super._(Errors.NotEnoughReplicasAfterAppend, response);
}

class InvalidRequiredAcksError extends KafkaError {
  InvalidRequiredAcksError(response)
      : super._(Errors.InvalidRequiredAcks, response);
}

class IllegalGenerationError extends KafkaError {
  IllegalGenerationError(response)
      : super._(Errors.IllegalGeneration, response);
}

class InconsistentGroupProtocolError extends KafkaError {
  InconsistentGroupProtocolError(response)
      : super._(Errors.InconsistentGroupProtocol, response);
}

class InvalidGroupIdError extends KafkaError {
  InvalidGroupIdError(response) : super._(Errors.InvalidGroupId, response);
}

class UnknownMemberIdError extends KafkaError {
  UnknownMemberIdError(response) : super._(Errors.UnknownMemberId, response);
}

class InvalidSessionTimeoutError extends KafkaError {
  InvalidSessionTimeoutError(response)
      : super._(Errors.InvalidSessionTimeout, response);
}

class RebalanceInProgressError extends KafkaError {
  RebalanceInProgressError(response)
      : super._(Errors.RebalanceInProgress, response);
}

class InvalidCommitOffsetSizeError extends KafkaError {
  InvalidCommitOffsetSizeError(response)
      : super._(Errors.InvalidCommitOffsetSize, response);
}

class TopicAuthorizationFailedError extends KafkaError {
  TopicAuthorizationFailedError(response)
      : super._(Errors.TopicAuthorizationFailed, response);
}

class GroupAuthorizationFailedError extends KafkaError {
  GroupAuthorizationFailedError(response)
      : super._(Errors.GroupAuthorizationFailed, response);
}

class ClusterAuthorizationFailedError extends KafkaError {
  ClusterAuthorizationFailedError(response)
      : super._(Errors.ClusterAuthorizationFailed, response);
}

class InvalidTimestampError extends KafkaError {
  InvalidTimestampError(response) : super._(Errors.InvalidTimestamp, response);
}

class UnsupportedSaslMechanismError extends KafkaError {
  UnsupportedSaslMechanismError(response)
      : super._(Errors.UnsupportedSaslMechanism, response);
}

class IllegalSaslStateError extends KafkaError {
  IllegalSaslStateError(response) : super._(Errors.IllegalSaslState, response);
}

class UnsupportedVersionError extends KafkaError {
  UnsupportedVersionError(response)
      : super._(Errors.UnsupportedVersion, response);
}
