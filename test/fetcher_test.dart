library kafka.test.fetcher;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

void main() {
  group('Fetcher:', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';
    Map<int, int> _expectedOffsets = new Map();
    List<TopicOffset> _initialOffsets = new List();

    setUp(() async {
      var host = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(host, 9092)]);
      var producer = new Producer(_session, 1, 100);
      var result = await producer.produce([
        new ProduceEnvelope(_topicName, 0, [new Message('msg1'.codeUnits)]),
        new ProduceEnvelope(_topicName, 1, [new Message('msg2'.codeUnits)]),
        new ProduceEnvelope(_topicName, 2, [new Message('msg3'.codeUnits)]),
      ]);
      _expectedOffsets = result.offsets[_topicName];
      result.offsets[_topicName].forEach((p, o) {
        _initialOffsets.add(new TopicOffset(_topicName, p, o));
      });
    });

    tearDown(() async {
      await _session.close();
    });

    test('it can consume exact number of messages from multiple brokers',
        () async {
      var fetcher = new Fetcher(_session, _initialOffsets);
      var fetchedCount = 0;
      await for (MessageEnvelope envelope in fetcher.fetch(limit: 3)) {
        expect(envelope.offset, _expectedOffsets[envelope.partitionId]);
        envelope.commit('');
        fetchedCount++;
      }
      expect(fetchedCount, equals(3));
    });

    test('it can handle cancelation request', () async {
      var fetcher = new Fetcher(_session, _initialOffsets);
      var fetchedCount = 0;
      await for (MessageEnvelope envelope in fetcher.fetch(limit: 3)) {
        expect(envelope.offset, _expectedOffsets[envelope.partitionId]);
        envelope.cancel();
        fetchedCount++;
      }
      expect(fetchedCount, equals(1));
    });

    test('it can resolve earliest offset', () async {
      var startOffsets = [new TopicOffset.earliest(_topicName, 0)];
      var fetcher = new Fetcher(_session, startOffsets);

      await for (MessageEnvelope envelope in fetcher.fetch(limit: 1)) {
        envelope.ack();
      }
    });
  });
}
