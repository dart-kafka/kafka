library kafka.test.api.produce;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

KafkaClient _client;
ProduceRequest _request;

void main() {
  setUp(() {
    var host = new KafkaHost('127.0.0.1', 9092);
    _client = new KafkaClient([host]);
    _request = new ProduceRequest(_client, host, 1, 1000);
  });

  test('it publishes messages to Kafka topic', () async {
    _request.addMessages('dartKafkaTest', 0, ['hello world']);
    var response = await _request.send();
    expect(response.topics, hasLength(1));
    expect(response.topics.first.topicName, equals('dartKafkaTest'));
    expect(response.topics.first.partitions.first.errorCode, equals(0));
    expect(
        response.topics.first.partitions.first.offset, greaterThanOrEqualTo(0));
  });
}
