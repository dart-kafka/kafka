library kafka.test.client;

import 'package:test/test.dart';
import 'package:semver/semver.dart';
import 'package:kafka/kafka.dart';

KafkaClient _client;

void main() {
  setUp(() {
    var version = new SemanticVersion.fromString('0.8.2');
    _client = new KafkaClient(version, [new KafkaHost('127.0.0.1', 9092)]);
  });

  test('it can fetch topic metadata', () async {
    var response = await _client.getMetadata();
    expect(response, new isInstanceOf<MetadataResponse>());
  });
}
