library kafka.test.api.consumer_metadata;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('ConsumerMetadataApi', () {
    KafkaClient _client;
    ConsumerMetadataRequest _request;

    setUp(() async {
      var ip = await getDefaultHost();
      var host = new KafkaHost(ip, 9092);
      _client = new KafkaClient([host]);
      _request = new ConsumerMetadataRequest(_client, host, 'testGroup');
    });

    tearDown(() async {
      await _client.close();
    });

    test('it fetches consumer metadata', () async {
      var response = await _request.send();
      expect(response.errorCode, equals(0));
      expect(response.coordinatorId, isNotNull);
      expect(response.coordinatorHost, isNotNull);
      expect(response.coordinatorPort, isNotNull);
    });
  });
}
