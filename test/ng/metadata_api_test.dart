import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Kafka.NG Metadata API: ', () {
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);

    tearDownAll(() async {
      await session.close();
    });

    test('we can send metadata requests to Kafka broker', () async {
      var request = new MetadataRequest();
      var response = await session.send(request, '127.0.0.1', 9092);
      expect(response, new isInstanceOf<MetadataResponse>());
      expect(response.brokers, hasLength(2));
      expect(response.topics, isList);
    });

    test('metadata response throws server error if present', () {
      var metadata = new TopicMetadata(Errors.InvalidTopic, 'test', []);
      expect(() {
        new MetadataResponse([], [metadata]);
      }, throwsA(new isInstanceOf<KafkaError>()));
    });
  });
}
