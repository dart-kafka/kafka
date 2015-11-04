library kafka.test.api.offset;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('OffsetApi', () {
    String _topicName = 'dartKafkaTest';
    KafkaSession _client;
    OffsetRequest _request;
    int _offset;

    setUp(() async {
      var ip = await getDefaultHost();
      var host = new KafkaHost(ip, 9092);
      _client = new KafkaSession([host]);
      var metadata = await _client.getMetadata();
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      var broker = metadata.brokers.firstWhere((b) => b.nodeId == leaderId);
      var leaderHost = new KafkaHost(broker.host, broker.port);

      ProduceRequest produce = new ProduceRequest(_client, leaderHost, 1, 1000);
      var now = new DateTime.now();
      var _message = 'test:' + now.toIso8601String();
      produce.addMessages(_topicName, 0, [new Message(_message.codeUnits)]);
      var response = await produce.send();
      _offset = response.topics.first.partitions.first.offset;
      _request = new OffsetRequest(_client, host, leaderId);
    });

    tearDown(() async {
      await _client.close();
    });

    test('it fetches offset info', () async {
      // TODO: finish test!
      _request.addTopicPartition(_topicName, 0, -1, 1);
      var response = await _request.send();

      expect(response.topics, hasLength(1));
    });
  });
}
