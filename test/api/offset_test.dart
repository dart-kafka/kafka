library kafka.test.api.offset;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

String _topicName = 'dartKafkaTest';
KafkaClient _client;
OffsetRequest _request;
int _offset;

void main() {
  setUp(() async {
    var ip = await getDefaultHost();
    var host = new KafkaHost(ip, 9092);
    _client = new KafkaClient([host]);
    var metadata = await _client.getMetadata();
    var leaderId = metadata.getTopicMetadata(_topicName).getPartition(0).leader;
    var broker = metadata.brokers.firstWhere((b) => b.nodeId == leaderId);
    var leaderHost = new KafkaHost(broker.host, broker.port);

    ProduceRequest produce = new ProduceRequest(_client, leaderHost, 1, 1000);
    var now = new DateTime.now();
    var _message = 'test:' + now.toIso8601String();
    produce.addMessages(_topicName, 0, [_message]);
    var response = await produce.send();
    _offset = response.topics.first.partitions.first.offset;
    _request = new OffsetRequest(_client, host, leaderId);
  });

  test('it fetches partition offset info', () async {
    _request.addTopicPartition(_topicName, 0, -1, 1);
    var response = await _request.send();

    expect(response.topics, hasLength(1));
  });
}
