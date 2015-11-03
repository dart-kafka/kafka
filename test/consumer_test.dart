library kafka.test.consumer;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

void main() {
  group('Consumer', () {
    KafkaClient _client;
    String _topicName = 'dartKafkaTest';
    Map<int, int> _expectedOffsets = new Map();

    setUp(() async {
      var host = await getDefaultHost();
      _client = new KafkaClient([new KafkaHost(host, 9092)]);
      var producer = new Producer(_client, 1, 100);
      producer.addMessages(_topicName, 0, [new Message('msg1'.codeUnits)]);
      producer.addMessages(_topicName, 1, [new Message('msg2'.codeUnits)]);
      producer.addMessages(_topicName, 2, [new Message('msg3'.codeUnits)]);
      var result = await producer.send();
      _expectedOffsets = result.offsets[_topicName];

      var offsets = new List<ConsumerOffset>();
      for (var p in _expectedOffsets.keys) {
        offsets.add(new ConsumerOffset(p, _expectedOffsets[p] - 1, ''));
      }
      var group = new ConsumerGroup(_client, 'cg');
      await group.commitOffsets({_topicName: offsets}, 0, '');
    });

    tearDown(() async {
      await _client.close();
    });

    test('it can consume messages from multiple brokers', () async {
      var topics = {
        _topicName: [0, 1, 2]
      };
      var consumer = new Consumer(
          _client, new ConsumerGroup(_client, 'cg'), topics, 100, 1);
      var consumedOffsets = new Map();
      await for (MessageEnvelope envelope in consumer.consume(limit: 3)) {
        consumedOffsets[envelope.partitionId] = envelope.offset;
        expect(envelope.offset, _expectedOffsets[envelope.partitionId]);
        envelope.ack('');
      }
      expect(consumedOffsets.length, _expectedOffsets.length);
    });
  });
}
