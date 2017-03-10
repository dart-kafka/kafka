import 'dart:async';

import 'package:logging/logging.dart';

import '../util/retry.dart';
import 'common.dart';
import 'consumer_metadata_api.dart';
import 'errors.dart';
import 'metadata_api.dart';
import 'session.dart';

final Logger _logger = new Logger('Metadata');

/// Provides information about topics and brokers in a Kafka cluster.
class Metadata {
  final Session session;

  Metadata(this.session);

  Future<Topics> fetchTopics(List<String> topics) {
    Future<Topics> fetch() {
      var req = new MetadataRequest(topics);
      var broker = session.contactPoints.first;
      return session
          .send(req, broker.host, broker.port)
          .then((response) => response.topics);
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 500),
        test: (err) => err is LeaderNotAvailableError);
  }

  Future<List<String>> listTopics() {
    var req = new MetadataRequest();
    var broker = session.contactPoints.first;
    return session.send(req, broker.host, broker.port).then((response) {
      return response.topics.names;
    });
  }

  Future<List<Broker>> listBrokers() {
    var req = new MetadataRequest();
    var broker = session.contactPoints.first;
    return session
        .send(req, broker.host, broker.port)
        .then((response) => response.brokers);
  }

  Future<Broker> fetchGroupCoordinator(String groupName) {
    Future<Broker> fetch() {
      _logger.info('Fetching group coordinator for group $groupName.');
      var req = new GroupCoordinatorRequest(groupName);
      var broker = session.contactPoints.first;
      return session.send(req, broker.host, broker.port).then((res) =>
          new Broker(
              res.coordinatorId, res.coordinatorHost, res.coordinatorPort));
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 500),
        test: (err) => err is ConsumerCoordinatorNotAvailableError);
  }

  Future<Map<TopicPartition, Broker>> fetchLeaders(
      List<TopicPartition> partitions) async {
    ///
  }
}
