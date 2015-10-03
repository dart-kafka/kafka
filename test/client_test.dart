library kafka.test.client;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

KafkaClient _client;

void main() {
  setUp(() async {
    var host = await getDefaultHost();
    _client = new KafkaClient([new KafkaHost(host, 9092)]);
  });

  test('it can fetch topic metadata', () async {
    var response = await _client.getMetadata();
    expect(response, new isInstanceOf<MetadataResponse>());
    expect(response.brokers, isNotEmpty);
  });
}
