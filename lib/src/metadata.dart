import 'dart:async';

import 'package:logging/logging.dart';

import 'common.dart';
import 'consumer_metadata_api.dart';
import 'errors.dart';
import 'metadata_api.dart';
import 'session.dart';
import 'util/retry.dart';

final Logger _logger = new Logger('Metadata');

/// Provides access to Kafka cluster metadata like list of nodes
/// in the cluster, topics and coordinators for consumer groups.
abstract class Metadata {
  /// Creates new instance of [Metadata] provider.
  ///
  /// Users shouldn't normally need to create a new instance themselves since
  /// [Session] provides access to one already via `metadata` field.
  ///
  /// List of [bootstrapServers] is used to establish connection with
  /// Kafka cluster. Each value in this list must be of format `host:port`.
  factory Metadata(List<String> bootstrapServers, Session session) {
    assert(bootstrapServers != null && bootstrapServers.isNotEmpty);

    List<Uri> bootstrapUris = bootstrapServers
        .map((_) => Uri.parse('kafka://$_'))
        .toList(growable: false);
    var isValid = bootstrapUris.every((_) => _.host != null && _.port != null);
    if (!isValid)
      throw new ArgumentError(
          'Invalid bootstrap servers list provided: $bootstrapServers');

    return new _Metadata(session, bootstrapUris);
  }

  Future<Topics> fetchTopics(List<String> topics);
  Future<List<String>> listTopics();
  Future<List<Broker>> listBrokers();
  Future<Broker> fetchGroupCoordinator(String groupName);
}

/// Default implementation of [Metadata] interface.
class _Metadata implements Metadata {
  final List<Uri> bootstrapUris;
  final Session session;

  _Metadata(this.session, this.bootstrapUris);

  Future<Topics> fetchTopics(List<String> topics) {
    Future<Topics> fetch() {
      var req = new MetadataRequest(topics);
      var broker = bootstrapUris.first;
      return session
          .send(req, broker.host, broker.port)
          .then((response) => response.topics);
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 500),
        test: (err) => err is LeaderNotAvailableError);
  }

  Future<List<String>> listTopics() {
    var req = new MetadataRequest();
    var broker = bootstrapUris.first;
    return session.send(req, broker.host, broker.port).then((response) {
      return response.topics.names;
    });
  }

  Future<List<Broker>> listBrokers() {
    var req = new MetadataRequest();
    var broker = bootstrapUris.first;
    return session
        .send(req, broker.host, broker.port)
        .then((response) => response.brokers);
  }

  Future<Broker> fetchGroupCoordinator(String groupName) {
    Future<Broker> fetch() {
      _logger.info('Fetching group coordinator for group $groupName.');
      var req = new GroupCoordinatorRequest(groupName);
      var broker = bootstrapUris.first;
      return session.send(req, broker.host, broker.port).then((res) =>
          new Broker(
              res.coordinatorId, res.coordinatorHost, res.coordinatorPort));
    }

    return retryAsync(fetch, 5, new Duration(milliseconds: 1000),
        test: (err) => err is ConsumerCoordinatorNotAvailableError);
  }
}
