library kafka.test.session;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

void main() {
  group('Session:', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';

    setUp(() async {
      var host = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(host, 9092)]);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it can list existing topics', () async {
      var topics = await _session.listTopics();
      expect(topics, new isInstanceOf<Set>());
      expect(topics, isNotEmpty);
      expect(topics, contains(_topicName));
    });

    test('it can fetch topic metadata', () async {
      var response = await _session.getMetadata([_topicName].toSet());
      expect(response, new isInstanceOf<ClusterMetadata>());
      expect(response.brokers, isNotEmpty);
      var topic = response.getTopicMetadata(_topicName);
      expect(topic, new isInstanceOf<TopicMetadata>());
      response = await _session.getMetadata([_topicName].toSet());
      var newTopic = response.getTopicMetadata(_topicName);
      expect(newTopic, same(topic));
    });

    test('it invalidates topic metadata', () async {
      var response = await _session.getMetadata([_topicName].toSet());
      var topic = response.getTopicMetadata(_topicName);
      response = await _session.getMetadata([_topicName].toSet(),
          invalidateCache: true);
      var newTopic = response.getTopicMetadata(_topicName);
      expect(newTopic, isNot(same(topic)));
    });

    test('it fetches topic metadata for auto-created topics', () async {
      var date = new DateTime.now().millisecondsSinceEpoch;
      var topicName = 'testTopic-${date}';
      var response = await _session.getMetadata([topicName].toSet());
      var topic = response.getTopicMetadata(topicName);
      expect(topic.errorCode, equals(KafkaServerError.NoError));
      expect(topic.partitions, isNotEmpty);
    });

    test('it can fetch consumer metadata', () async {
      var response = await _session.getConsumerMetadata('testGroup');
      expect(response.errorCode, equals(0));
      expect(response.coordinatorId, isNotNull);
      expect(response.coordinatorHost, isNotNull);
      expect(response.coordinatorPort, isNotNull);
    });
  });
}
