import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('Metadata:', () {
    Session session = new Session([new ContactPoint('127.0.0.1:9092')]);
    Metadata metadata = new Metadata(session);

    tearDownAll(() async {
      await session.close();
    });

    test('it can fetch specific topic metadata', () async {
      var topics = await metadata.fetchTopics(['testTopic']);
      expect(topics, new isInstanceOf<Topics>());
      expect(topics, hasLength(1));
      expect(topics['testTopic'], isNotNull);
      expect(topics['testTopic'].toString(),
          contains('Topic{testTopic, error: 0;'));
    });

    test('it can list existing topics', () async {
      var topics = await metadata.listTopics();
      expect(topics, isList);
      expect(topics, isNotEmpty);
    });

    test('it can list Kafka brokers within cluster', () async {
      var brokers = await metadata.listBrokers();
      expect(brokers, isList);
      expect(brokers, hasLength(2));
    });

    test('it can fetch group coordinator', () async {
      var group =
          'testGroup' + (new DateTime.now()).millisecondsSinceEpoch.toString();
      var broker = await metadata.fetchGroupCoordinator(group);
      expect(broker, new isInstanceOf<Broker>());
      expect(broker.id, isNotNull);
      expect(broker.host, isNotNull);
      expect(broker.port, isNotNull);
    });

    // test('it can fetch topic metadata', () async {
    //   var response = await _session.getMetadata([_topicName].toSet());
    //   expect(response, new isInstanceOf<ClusterMetadata>());
    //   expect(response.brokers, isNotEmpty);
    //   var topic = response.getTopicMetadata(_topicName);
    //   expect(topic, new isInstanceOf<TopicMetadata>());
    //   response = await _session.getMetadata([_topicName].toSet());
    //   var newTopic = response.getTopicMetadata(_topicName);
    //   expect(newTopic, same(topic));
    // });
    //
    // test('it fetches topic metadata for auto-created topics', () async {
    //   var date = new DateTime.now().millisecondsSinceEpoch;
    //   var topicName = 'testTopic-${date}';
    //   var response = await _session.getMetadata([topicName].toSet());
    //   var topic = response.getTopicMetadata(topicName);
    //   expect(topic.errorCode, equals(KafkaServerError.NoError_));
    //   expect(topic.partitions, isNotEmpty);
    // });
  });
}
