library kafka.test.api.fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('FetchApi', () {
    String _topicName = 'dartKafkaTest';
    KafkaClient _client;
    FetchRequest _request;
    String _message;
    int _offset;

    setUp(() async {
      var ip = await getDefaultHost();
      var host = new KafkaHost(ip, 9092);
      _client = new KafkaClient([host]);
      var metadata = await _client.getMetadata();
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      var broker = metadata.brokers.firstWhere((b) => b.nodeId == leaderId);
      var leaderHost = new KafkaHost(broker.host, broker.port);

      ProduceRequest produce = new ProduceRequest(_client, leaderHost, 1, 1000);
      var now = new DateTime.now();
      _message = 'test:' + now.toIso8601String();
      produce.addMessages(_topicName, 0, [new Message(_message.codeUnits)]);
      var response = await produce.send();
      _offset = response.topics.first.partitions.first.offset;
      _request = new FetchRequest(_client, leaderHost, 100, 1);
    });

    test('it fetches messages from Kafka topic', () async {
      _request.add(_topicName, 0, _offset);
      var response = await _request.send();

      expect(response.topics, hasLength(1));
      expect(response.topics[_topicName].first.messages,
          hasLength(greaterThanOrEqualTo(1)));
      var value =
          response.topics[_topicName].first.messages.messages[_offset].value;
      var text = new String.fromCharCodes(value);
      expect(text, equals(_message));
    });
  });
}
