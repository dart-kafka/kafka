library kafka.test.api.produce;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/src/protocol.dart';
import '../setup.dart';

void main() {
  group('ProduceApi', () {
    String _topicName = 'dartKafkaTest';
    KafkaHost _host;
    KafkaSession _session;
    ProduceRequest _request;

    setUp(() async {
      var ip = await getDefaultHost();
      var host = new KafkaHost(ip, 9092);
      _session = new KafkaSession([host]);
      var metadata = await _session.getMetadata();
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      var broker = metadata.brokers.firstWhere((b) => b.nodeId == leaderId);
      _host = new KafkaHost(broker.host, broker.port);
      _request = new ProduceRequest(1, 1000);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it publishes messages to Kafka topic', () async {
      _request.addMessages(
          _topicName, 0, [new Message('hello world'.codeUnits)]);
      var response = await _session.send(_host, _request);
      expect(response.topics, hasLength(1));
      expect(response.topics.first.topicName, equals(_topicName));
      expect(response.topics.first.partitions.first.errorCode, equals(0));
      expect(response.topics.first.partitions.first.offset,
          greaterThanOrEqualTo(0));
    });
  });
}
