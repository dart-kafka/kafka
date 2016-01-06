library kafka.test.api.produce;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('ProduceApi:', () {
    String _topicName = 'dartKafkaTest';
    Broker _broker;
    KafkaSession _session;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var metadata = await _session.getMetadata([_topicName].toSet());
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      _broker = metadata.getBroker(leaderId);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it publishes messages to Kafka topic', () async {
      var request = new ProduceRequest(1, 1000, [
        new ProduceEnvelope(
            _topicName, 0, [new Message('hello world'.codeUnits)])
      ]);
      ProduceResponse response = await _session.send(_broker, request);
      expect(response.results, hasLength(1));
      expect(response.results.first.topicName, equals(_topicName));
      expect(response.results.first.errorCode, equals(0));
      expect(response.results.first.offset, greaterThanOrEqualTo(0));
    });

    test('it publishes GZip encoded messages to Kafka topic', () async {
      var request = new ProduceRequest(1, 1000, [
        new ProduceEnvelope(
            _topicName,
            0,
            [
              new Message('hello world'.codeUnits),
              new Message('peace and love'.codeUnits)
            ],
            compression: KafkaCompression.gzip)
      ]);
      ProduceResponse response = await _session.send(_broker, request);
      expect(response.results, hasLength(1));
      expect(response.results.first.topicName, equals(_topicName));
      expect(response.results.first.errorCode, equals(0));
      expect(response.results.first.offset, greaterThanOrEqualTo(0));
    });
  });
}
