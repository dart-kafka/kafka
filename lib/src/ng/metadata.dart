import 'dart:async';

import '../util/retry.dart';
import 'common.dart';
import 'consumer_metadata_api.dart';
import 'errors.dart';
import 'metadata_api.dart';
import 'session.dart';

class KMetadata {
  final KSession session;

  KMetadata({KSession session})
      : session = (session is KSession) ? session : KAFKA_DEFAULT_SESSION;

  Future<List<TopicMetadata>> fetchTopics(List<String> topics) {
    Future<List<TopicMetadata>> fetch() {
      var req = new MetadataRequestV0(topics);
      var broker = session.contactPoints.first;
      return session
          .send(req, broker.host, broker.port)
          .then((response) => response.topics);
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 500),
        test: (err) => err is LeaderNotAvailableError);
  }

  Future<List<String>> listTopics() {
    var req = new MetadataRequestV0();
    var broker = session.contactPoints.first;
    return session.send(req, broker.host, broker.port).then((response) {
      return response.topics.map((_) => _.topicName).toList();
    });
  }

  Future<List<Broker>> listBrokers() {
    var req = new MetadataRequestV0();
    var broker = session.contactPoints.first;
    return session
        .send(req, broker.host, broker.port)
        .then((response) => response.brokers);
  }

  Future<Broker> fetchGroupCoordinator(String groupName) {
    Future<Broker> fetch() {
      var req = new GroupCoordinatorRequestV0(groupName);
      var broker = session.contactPoints.first;
      return session.send(req, broker.host, broker.port).then((res) =>
          new Broker(
              res.coordinatorId, res.coordinatorHost, res.coordinatorPort));
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 500),
        test: (err) => err is ConsumerCoordinatorNotAvailableError);
  }
}
