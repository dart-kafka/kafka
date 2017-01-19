import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Protocol.NG Metadata API: ', () {
    test('we can send metadata requests to Kafka broker', () async {
      var session = new KSession();
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
