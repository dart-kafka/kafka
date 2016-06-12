library kafka.common.errors.test;

import 'package:test/test.dart';
import 'package:kafka/common.dart';

void main() {
  group('KafkaServerError:', () {
    test('it handles error codes correctly', () {
      expect(new KafkaServerError(0).isNoError, isTrue);
      expect(new KafkaServerError(-1).isUnknown, isTrue);
      expect(new KafkaServerError(-1).isError, isTrue);
      expect(new KafkaServerError(1).isOffsetOutOfRange, isTrue);
      expect(new KafkaServerError(2).isInvalidMessage, isTrue);
      expect(new KafkaServerError(3).isUnknownTopicOrPartition, isTrue);
      expect(new KafkaServerError(4).isInvalidMessageSize, isTrue);
      expect(new KafkaServerError(5).isLeaderNotAvailable, isTrue);
      expect(new KafkaServerError(6).isNotLeaderForPartition, isTrue);
      expect(new KafkaServerError(7).isRequestTimedOut, isTrue);
      expect(new KafkaServerError(8).isBrokerNotAvailable, isTrue);
      expect(new KafkaServerError(9).isReplicaNotAvailable, isTrue);
      expect(new KafkaServerError(10).isMessageSizeTooLarge, isTrue);
      expect(new KafkaServerError(11).isStaleControllerEpoch, isTrue);
      expect(new KafkaServerError(12).isOffsetMetadataTooLarge, isTrue);
      expect(new KafkaServerError(14).isOffsetsLoadInProgress, isTrue);
      expect(
          new KafkaServerError(15).isConsumerCoordinatorNotAvailable, isTrue);
      expect(new KafkaServerError(16).isNotCoordinatorForConsumer, isTrue);
    });

    test('it can be converted to string', () {
      expect(
          new KafkaServerError(0).toString(), 'KafkaServerError: NoError(0)');
    });

    test('it provides error message', () {
      expect(new KafkaServerError(1).message, 'OffsetOutOfRange');
    });
  });
}
