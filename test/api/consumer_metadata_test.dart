library kafka.test.api.consumer_metadata;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('ConsumerMetadataApi', () {
    KafkaSession _session;
    ConsumerMetadataRequest _request;

    setUp(() async {
      var ip = await getDefaultHost();
      var host = new KafkaHost(ip, 9092);
      _session = new KafkaSession([host]);
      _request = new ConsumerMetadataRequest(_session, host, 'testGroup');
    });

    tearDown(() async {
      await _session.close();
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
