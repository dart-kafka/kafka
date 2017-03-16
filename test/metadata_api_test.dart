import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('Kafka.NG Metadata API: ', () {
    Session session = new Session(['127.0.0.1:9092']);

    tearDownAll(() async {
      await session.close();
    });

    test('we can send metadata requests to Kafka broker', () async {
      var request = new MetadataRequest();
      var response = await session.send(request, '127.0.0.1', 9092);
      expect(response, new isInstanceOf<MetadataResponse>());
      expect(response.brokers, hasLength(2));
      expect(response.topics, new isInstanceOf<Topics>());
    });

    test('metadata response throws server error if present', () {
      var metadata = new Topic(Errors.InvalidTopic, 'test', new Partitions([]));
      expect(() {
        new MetadataResponse([], new Topics([metadata], new Brokers([])));
      }, throwsA(new isInstanceOf<KafkaError>()));
    });
  });
}
