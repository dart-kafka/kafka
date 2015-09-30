library kafka.test.api.offset;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

String _topicName = 'dartKafkaTest';
KafkaClient _client;
OffsetRequest _request;
int _offset;

void main() {
  setUp(() async {
    var host = new KafkaHost('127.0.0.1', 9092);
    _client = new KafkaClient('0.8.2', [host]);
    var metadata = await _client.getMetadata();
    var replicaId = metadata.brokers.first.nodeId;

    ProduceRequest produce = new ProduceRequest(_client, host, 1, 1000);
    var now = new DateTime.now();
    var _message = 'test:' + now.toIso8601String();
    produce.addMessages(_topicName, 0, [_message]);
    var response = await produce.send();
    _offset = response.topics.first.partitions.first.offset;
    _request = new OffsetRequest(_client, host, replicaId);
  });

  test('it fetches partition offset info', () async {
    _request.addTopicPartition(_topicName, 0, -1, 1);
    var response = await _request.send();

    expect(response.topics, hasLength(1));
  });
}
