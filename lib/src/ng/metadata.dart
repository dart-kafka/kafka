import 'dart:async';

import '../util/retry.dart';
import 'common.dart';
import 'consumer_metadata_api.dart';
import 'errors.dart';
import 'metadata_api.dart';
import 'session.dart';

class _ContactPoint {
  final String host;
  final int port;

  _ContactPoint._(this.host, this.port);

  factory _ContactPoint(String uri) {
    var u = Uri.parse('kafka://' + uri);
    return new _ContactPoint._(u.host, u.port ?? 9092);
  }
}

class KMetadata {
  final KSession session;
  List<_ContactPoint> _contactPoints;

  KMetadata(this.session, List<String> contactPoints) {
    if (contactPoints == null || contactPoints.isEmpty) {
      throw new ArgumentError(
          'Must provide at least one contact point to Kafka cluster.');
    }
    _contactPoints = contactPoints.map((_) => new _ContactPoint(_)).toList();
  }

  Future<List<TopicMetadata>> fetchTopics(List<String> topics) {
    Future<List<TopicMetadata>> fetch() {
      var req = new MetadataRequestV0(topics);
      var host = _contactPoints.first;
      return session
          .send(req, host.host, host.port)
          .then((response) => response.topics);
    }

    return retryAsync(fetch, 5, 500,
        errorCondition: (err) => err is LeaderNotAvailableError);
  }

  Future<List<String>> listTopics() {
    var req = new MetadataRequestV0();
    var host = _contactPoints.first;
    return session.send(req, host.host, host.port).then((response) {
      return response.topics.map((_) => _.topicName).toList();
    });
  }

  Future<List<Broker>> listBrokers() {
    var req = new MetadataRequestV0();
    var host = _contactPoints.first;
    return session
        .send(req, host.host, host.port)
        .then((response) => response.brokers);
  }

  Future<Broker> fetchGroupCoordinator(String groupName) {
    Future<Broker> fetch() {
      var req = new GroupCoordinatorRequestV0(groupName);
      var host = _contactPoints.first;
      return session.send(req, host.host, host.port).then((res) => new Broker(
          res.coordinatorId, res.coordinatorHost, res.coordinatorPort));
    }

    return retryAsync(fetch, 5, 500,
        errorCondition: (err) => err is ConsumerCoordinatorNotAvailableError);
  }
}
