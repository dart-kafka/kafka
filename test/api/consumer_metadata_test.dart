library kafka.test.api.consumer_metadata;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

void main() {
  group('ConsumerMetadataApi', () {
    KafkaHost _host;
    KafkaSession _session;
    ConsumerMetadataRequest _request;

    setUp(() async {
      var ip = await getDefaultHost();
      _host = new KafkaHost(ip, 9092);
      _session = new KafkaSession([_host]);
      _request = new ConsumerMetadataRequest('testGroup');
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches consumer metadata', () async {
      var response = await _session.send(_host, _request);
      expect(response.errorCode, equals(0));
      expect(response.coordinatorId, isNotNull);
      expect(response.coordinatorHost, isNotNull);
      expect(response.coordinatorPort, isNotNull);
    });
  });
}
