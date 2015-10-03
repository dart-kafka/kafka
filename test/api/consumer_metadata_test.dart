library kafka.test.api.consumer_metadata;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import '../setup.dart';

KafkaClient _client;
ConsumerMetadataRequest _request;

void main() {
  setUp(() async {
    var ip = await getDefaultHost();
    var host = new KafkaHost(ip, 9092);
    _client = new KafkaClient([host]);
    _request = new ConsumerMetadataRequest(_client, host, 'testGroup');
  });

  test('it fetches consumer metadata', () async {
    var response = await _request.send();
    expect(response.errorCode, equals(0));
    expect(response.coordinatorId, isNotNull);
    expect(response.coordinatorHost, isNotNull);
    expect(response.coordinatorPort, isNotNull);
  });
}
