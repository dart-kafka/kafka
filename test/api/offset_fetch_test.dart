library kafka.test.api.offset_fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

KafkaClient _client;
OffsetFetchRequest _request;
KafkaHost _host;
KafkaHost _coordinatorHost;
String _testGroup;

void main() {
  setUp(() async {
    var ip = await getDefaultHost();
    _host = new KafkaHost(ip, 9092);
    _client = new KafkaClient([_host]);
    var now = new DateTime.now();
    _testGroup = 'group:' + now.millisecondsSinceEpoch.toString();
    var consumerMetadataRequest =
        new ConsumerMetadataRequest(_client, _host, _testGroup);
    var metadata = await consumerMetadataRequest.send();
    _coordinatorHost =
        new KafkaHost(metadata.coordinatorHost, metadata.coordinatorPort);
    _request = new OffsetFetchRequest(_client, _coordinatorHost, _testGroup);
  });

  test('it fetches consumer offsets', () async {
    _request.addTopicPartitions('dartKafkaTest', [0]);
    var response = await _request.send();
    expect(response.topics, hasLength(equals(1)));
    expect(response.topics, contains('dartKafkaTest'));
    var partitions = response.topics['dartKafkaTest'];
    expect(partitions, hasLength(1));
    var p = partitions.first;
    expect(p.errorCode, equals(0));
  });
}
