import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

void main() {
  group('Versions API', () {
    Session session = new Session(['127.0.0.1:9092']);

    tearDownAll(() async {
      await session.close();
    });

    test('can obtain supported api versions from Kafka cluster', () async {
      var request = new ApiVersionsRequest();
      var response = await session.send(request, '127.0.0.1', 9092);
      expect(response, isA<ApiVersionsResponse>());
      expect(response.error, 0);
      expect(response.versions, isNotEmpty);
    });
  });
}
