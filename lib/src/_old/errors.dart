library kafka.errors;

import 'common.dart';

/// Used to indicate there is a mismatch in CRC sum of a message (message is
/// corrupted).
class MessageCrcMismatchError extends StateError {
  MessageCrcMismatchError(String message) : super(message);
}

/// Represents errors returned by Kafka server.
class KafkaServerError {
  static const int NoError_ = 0;
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

  /// Numeric code of this server error.
  final int code;

  final response;

  KafkaServerError._(this.code, this.response);

  factory KafkaServerError.fromCode(int code, response) {
    switch (code) {
      case NoError_:
        return new NoError(response);
      case Unknown:
        return new UnknownError(response);
      case OffsetOutOfRange:
        return new OffsetOutOfRangeError(response, null);
      case InvalidMessage:
        return new InvalidMessageError(response);
      case UnknownTopicOrPartition:
        return new UnknownTopicOrPartitionError(response);
      case InvalidMessageSize:
        return new InvalidMessageSizeError(response);
      case LeaderNotAvailable:
        return new LeaderNotAvailableError(response);
      case NotLeaderForPartition:
        return new NotLeaderForPartitionError(response);
      case RequestTimedOut:
        return new RequestTimedOutError(response);
      case BrokerNotAvailable:
        return new BrokerNotAvailableError(response);
      case ReplicaNotAvailable:
        return new ReplicaNotAvailableError(response);
      case MessageSizeTooLarge:
        return new MessageSizeTooLargeError(response);
      case StaleControllerEpoch:
        return new StaleControllerEpochError(response);
      case OffsetMetadataTooLarge:
        return new OffsetMetadataTooLargeError(response);
      case OffsetsLoadInProgress:
        return new OffsetsLoadInProgressError(response);
      case ConsumerCoordinatorNotAvailable:
        return new ConsumerCoordinatorNotAvailableError(response);
      case NotCoordinatorForConsumer:
        return new NotCoordinatorForConsumerError(response);
      case InvalidTopic:
        return new InvalidTopicError(response);
      case RecordListTooLarge:
        return new RecordListTooLargeError(response);
      case NotEnoughReplicas:
        return new NotEnoughReplicasError(response);
      case NotEnoughReplicasAfterAppend:
        return new NotEnoughReplicasAfterAppendError(response);
      case InvalidRequiredAcks:
        return new InvalidRequiredAcksError(response);
      case IllegalGeneration:
        return new IllegalGenerationError(response);
      case InconsistentGroupProtocol:
        return new InconsistentGroupProtocolError(response);
      case InvalidGroupId:
        return new InvalidGroupIdError(response);
      case UnknownMemberId:
        return new UnknownMemberIdError(response);
      case InvalidSessionTimeout:
        return new InvalidSessionTimeoutError(response);
      case RebalanceInProgress:
        return new RebalanceInProgressError(response);
      case InvalidCommitOffsetSize:
        return new InvalidCommitOffsetSizeError(response);
      case TopicAuthorizationFailed:
        return new TopicAuthorizationFailedError(response);
      case GroupAuthorizationFailed:
        return new GroupAuthorizationFailedError(response);
      case ClusterAuthorizationFailed:
        return new ClusterAuthorizationFailedError(response);
      case InvalidTimestamp:
        return new InvalidTimestampError(response);
      case UnsupportedSaslMechanism:
        return new UnsupportedSaslMechanismError(response);
      case IllegalSaslState:
        return new IllegalSaslStateError(response);
      case UnsupportedVersion:
        return new UnsupportedVersionError(response);
      default:
        throw new ArgumentError('Unsupported Kafka server error code $code.');
    }
  }

  @override
  String toString() => 'KafkaServerError: ${runtimeType}(${code})';
}

class NoError extends KafkaServerError {
  NoError(response) : super._(KafkaServerError.NoError_, response);
}

class UnknownError extends KafkaServerError {
  UnknownError(response) : super._(KafkaServerError.Unknown, response);
}

class OffsetOutOfRangeError extends KafkaServerError {
  final List<TopicPartition> topicPartitions;
  OffsetOutOfRangeError(response, this.topicPartitions)
      : super._(KafkaServerError.OffsetOutOfRange, response);
}

class InvalidMessageError extends KafkaServerError {
  InvalidMessageError(response)
      : super._(KafkaServerError.InvalidMessage, response);
}

class UnknownTopicOrPartitionError extends KafkaServerError {
  UnknownTopicOrPartitionError(response)
      : super._(KafkaServerError.UnknownTopicOrPartition, response);
}

class InvalidMessageSizeError extends KafkaServerError {
  InvalidMessageSizeError(response)
      : super._(KafkaServerError.InvalidMessageSize, response);
}

class LeaderNotAvailableError extends KafkaServerError {
  LeaderNotAvailableError(response)
      : super._(KafkaServerError.LeaderNotAvailable, response);
}

class NotLeaderForPartitionError extends KafkaServerError {
  NotLeaderForPartitionError(response)
      : super._(KafkaServerError.NotLeaderForPartition, response);
}

class RequestTimedOutError extends KafkaServerError {
  RequestTimedOutError(response)
      : super._(KafkaServerError.RequestTimedOut, response);
}

class BrokerNotAvailableError extends KafkaServerError {
  BrokerNotAvailableError(response)
      : super._(KafkaServerError.BrokerNotAvailable, response);
}

class ReplicaNotAvailableError extends KafkaServerError {
  ReplicaNotAvailableError(response)
      : super._(KafkaServerError.ReplicaNotAvailable, response);
}

class MessageSizeTooLargeError extends KafkaServerError {
  MessageSizeTooLargeError(response)
      : super._(KafkaServerError.MessageSizeTooLarge, response);
}

class StaleControllerEpochError extends KafkaServerError {
  StaleControllerEpochError(response)
      : super._(KafkaServerError.StaleControllerEpoch, response);
}

class OffsetMetadataTooLargeError extends KafkaServerError {
  OffsetMetadataTooLargeError(response)
      : super._(KafkaServerError.OffsetMetadataTooLarge, response);
}

class OffsetsLoadInProgressError extends KafkaServerError {
  OffsetsLoadInProgressError(response)
      : super._(KafkaServerError.OffsetsLoadInProgress, response);
}

class ConsumerCoordinatorNotAvailableError extends KafkaServerError {
  ConsumerCoordinatorNotAvailableError(response)
      : super._(KafkaServerError.ConsumerCoordinatorNotAvailable, response);
}

class NotCoordinatorForConsumerError extends KafkaServerError {
  NotCoordinatorForConsumerError(response)
      : super._(KafkaServerError.NotCoordinatorForConsumer, response);
}

class InvalidTopicError extends KafkaServerError {
  InvalidTopicError(response)
      : super._(KafkaServerError.InvalidTopic, response);
}

class RecordListTooLargeError extends KafkaServerError {
  RecordListTooLargeError(response)
      : super._(KafkaServerError.RecordListTooLarge, response);
}

class NotEnoughReplicasError extends KafkaServerError {
  NotEnoughReplicasError(response)
      : super._(KafkaServerError.NotEnoughReplicas, response);
}

class NotEnoughReplicasAfterAppendError extends KafkaServerError {
  NotEnoughReplicasAfterAppendError(response)
      : super._(KafkaServerError.NotEnoughReplicasAfterAppend, response);
}

class InvalidRequiredAcksError extends KafkaServerError {
  InvalidRequiredAcksError(response)
      : super._(KafkaServerError.InvalidRequiredAcks, response);
}

class IllegalGenerationError extends KafkaServerError {
  IllegalGenerationError(response)
      : super._(KafkaServerError.IllegalGeneration, response);
}

class InconsistentGroupProtocolError extends KafkaServerError {
  InconsistentGroupProtocolError(response)
      : super._(KafkaServerError.InconsistentGroupProtocol, response);
}

class InvalidGroupIdError extends KafkaServerError {
  InvalidGroupIdError(response)
      : super._(KafkaServerError.InvalidGroupId, response);
}

class UnknownMemberIdError extends KafkaServerError {
  UnknownMemberIdError(response)
      : super._(KafkaServerError.UnknownMemberId, response);
}

class InvalidSessionTimeoutError extends KafkaServerError {
  InvalidSessionTimeoutError(response)
      : super._(KafkaServerError.InvalidSessionTimeout, response);
}

class RebalanceInProgressError extends KafkaServerError {
  RebalanceInProgressError(response)
      : super._(KafkaServerError.RebalanceInProgress, response);
}

class InvalidCommitOffsetSizeError extends KafkaServerError {
  InvalidCommitOffsetSizeError(response)
      : super._(KafkaServerError.InvalidCommitOffsetSize, response);
}

class TopicAuthorizationFailedError extends KafkaServerError {
  TopicAuthorizationFailedError(response)
      : super._(KafkaServerError.TopicAuthorizationFailed, response);
}

class GroupAuthorizationFailedError extends KafkaServerError {
  GroupAuthorizationFailedError(response)
      : super._(KafkaServerError.GroupAuthorizationFailed, response);
}

class ClusterAuthorizationFailedError extends KafkaServerError {
  ClusterAuthorizationFailedError(response)
      : super._(KafkaServerError.ClusterAuthorizationFailed, response);
}

class InvalidTimestampError extends KafkaServerError {
  InvalidTimestampError(response)
      : super._(KafkaServerError.InvalidTimestamp, response);
}

class UnsupportedSaslMechanismError extends KafkaServerError {
  UnsupportedSaslMechanismError(response)
      : super._(KafkaServerError.UnsupportedSaslMechanism, response);
}

class IllegalSaslStateError extends KafkaServerError {
  IllegalSaslStateError(response)
      : super._(KafkaServerError.IllegalSaslState, response);
}

class UnsupportedVersionError extends KafkaServerError {
  UnsupportedVersionError(response)
      : super._(KafkaServerError.UnsupportedVersion, response);
}
