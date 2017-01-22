import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Kafka.NG Metadata API: ', () {
    KSession session;
    setUp(() {
      session = new KSession();
    });

    tearDown(() {
      session.close();
    });

    test('we can send metadata requests to Kafka broker', () async {
      var request = new MetadataRequestV0();
      var response = await session.send(request, '127.0.0.1', 9092);
      expect(response, new isInstanceOf<MetadataResponseV0>());
      expect(response.brokers, hasLength(2));
      expect(response.topics, isList);
    });

    test('metadata response throws server error if present', () {
      var metadata =
          new TopicMetadata(KafkaServerError.InvalidTopic, 'test', []);
      expect(() {
        new MetadataResponseV0([], [metadata]);
      }, throwsA(new isInstanceOf<KafkaServerError>()));
    });
  });
}
