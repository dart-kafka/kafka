library kafka.test.api.offset_fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

KafkaClient _client;
OffsetFetchRequest _request;

void main() {
  setUp(() {
    var host = new KafkaHost('127.0.0.1', 9092);
    _client = new KafkaClient([host]);
    _request = new OffsetFetchRequest(_client, host, 'testGroup');
  });

  test('it fetches consumer offsets', () async {
    _request.addTopicPartitions('dartKafkaTest', [0]);
    var response = await _request.send();
    expect(response.topics, hasLength(equals(1)));
    expect(response.topics, contains('dartKafkaTest'));
    var partitions = response.topics['dartKafkaTest'];
    expect(partitions, hasLength(1));
    var p = partitions.first;
    expect(p.errorCode, equals(0));
  });
}
