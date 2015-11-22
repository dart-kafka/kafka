library kafka.test.api.offset_fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/src/protocol.dart';
import '../setup.dart';

void main() {
  group('OffsetFetchApi', () {
    KafkaSession _session;
    OffsetFetchRequest _request;
    KafkaHost _host;
    KafkaHost _coordinatorHost;
    String _testGroup;

    setUp(() async {
      var ip = await getDefaultHost();
      _host = new KafkaHost(ip, 9092);
      _session = new KafkaSession([_host]);
      var now = new DateTime.now();
      _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
      var metadata = await _session.getConsumerMetadata(_testGroup);
      _coordinatorHost =
          new KafkaHost(metadata.coordinatorHost, metadata.coordinatorPort);
      _request = new OffsetFetchRequest(_testGroup, {
        'dartKafkaTest': new Set.from([0])
      });
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches consumer offsets', () async {
      var response = await _session.send(_coordinatorHost, _request);
      expect(response.offsets, hasLength(equals(1)));
      expect(response.offsets, contains('dartKafkaTest'));
      var partitions = response.offsets['dartKafkaTest'];
      expect(partitions, hasLength(1));
      var p = partitions.first;
      expect(p.errorCode, equals(0));
    });
  });
}
