library kafka.test.api.offset_commit;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

String _topicName = 'dartKafkaTest';
KafkaClient _client;
KafkaHost _host;
OffsetCommitRequest _request;
int _offset;
String _testGroup;

void main() {
  setUp(() async {
    _host = new KafkaHost('127.0.0.1', 9092);
    _client = new KafkaClient([_host]);

    ProduceRequest produce = new ProduceRequest(_client, _host, 1, 1000);
    var now = new DateTime.now();
    var message = 'test:' + now.toIso8601String();
    produce.addMessages(_topicName, 0, [message]);
    var response = await produce.send();
    _offset = response.topics.first.partitions.first.offset;

    _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
    _request = new OffsetCommitRequest(_client, _host, _testGroup, 0, '');
  });

  test('it fetches consumer offsets', () async {
    var now = new DateTime.now();
    _request.addTopicPartitionOffset(
        'dartKafkaTest', 0, _offset, now.millisecondsSinceEpoch, 'helloworld');
    var response = await _request.send();
    expect(response.topics, hasLength(equals(1)));
    expect(response.topics, contains('dartKafkaTest'));
    var partitions = response.topics['dartKafkaTest'];
    expect(partitions, hasLength(1));
    var p = partitions.first;
    expect(p.errorCode, equals(0));

    var fetch = new OffsetFetchRequest(_client, _host, _testGroup);
    fetch.addTopicPartitions(_topicName, [0]);
    var fetchResponse = await fetch.send();
    var info = fetchResponse.topics[_topicName].first;
    expect(info.errorCode, equals(0));
    expect(info.offset, equals(_offset));
    expect(info.metadata, equals('helloworld'));
  });
}
