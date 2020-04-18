import 'dart:async';

import 'package:logging/logging.dart';

import 'io.dart';
import 'metadata.dart';
import 'versions_api.dart';
import 'versions.dart';

final Logger _logger = new Logger('Session');

/// Session responsible for handling connections to all brokers in a Kafka
/// cluster.
///
/// Handles resolution of supported API versions by the server and the client.
/// The latest API version supported by both sides is used.
abstract class Session {
  /// Creates new connection session to a Kafka cluster.
  ///
  /// The list of [bootstrapServers] is used initially to establish connection
  /// to at least one node in the cluster. Full list of nodes is discovered
  /// from metadata information fetched from the bootstrap node.
  factory Session(List<String> bootstrapServers) {
    return new _SessionImpl(bootstrapServers);
  }

  /// Provides access to metadata about the Kafka cluster this session is
  /// connected to.
  Metadata get metadata;

  /// Sends [request] to a broker specified by [host] and [port].
  Future<T> send<T>(KRequest<T> request, String host, int port);

  /// Closes all open connections to Kafka brokers.
  ///
  /// Waits for any in-flight requests to complete before closing connections.
  /// After calling `close()` no new requests are accepted by this session.
  Future close();
}

class _SessionImpl implements Session {
  final Map<String, Future<KSocket>> _sockets = new Map();
  Metadata _metadata;
  Metadata get metadata => _metadata;

  /// Resolved API versions supported by both server and client.
  Map<int, int> _apiVersions;
  Completer _apiResolution;

  _SessionImpl(List<String> bootstrapServers) {
    _metadata = new Metadata(bootstrapServers, this);
  }

  Future<T> send<T>(KRequest<T> request, String host, int port) {
    /// TODO: Find a way to perform `socket.sendPacket()` without async gap
    Future<KSocket> result;
    if (_apiVersions == null) {
      /// TODO: resolve versions for each node separately
      /// It may not always be true that all nodes in Kafka cluster
      /// support exactly the same versions, e.g. during server upgrades.
      /// Might have to move this logic to [KSocket] (?).
      result =
          _resolveApiVersions(host, port).then((_) => _getSocket(host, port));
    } else {
      result = _getSocket(host, port);
    }

    return result.then((socket) {
      var version = _apiVersions[request.apiKey];
      _logger.finest('Sending $request (v$version) to $host:$port');
      var payload = request.encoder.encode(request, version);
      return socket.sendPacket(request.apiKey, version, payload);
    }).then((responseData) {
      // var version = _apiVersions[request.apiKey];

      /// TODO: supply api version to response decoders.
      return request.decoder.decode(responseData);
    });
  }

  Future _resolveApiVersions(String host, int port) {
    if (_apiResolution != null) return _apiResolution.future;
    _apiResolution = new Completer();
    var request = new ApiVersionsRequest();
    _getSocket(host, port).then((socket) {
      var payload = request.encoder.encode(request, 0);
      return socket.sendPacket(request.apiKey, 0, payload);
    }).then((data) {
      var response = request.decoder.decode(data);
      _apiVersions = resolveApiVersions(response.versions, supportedVersions);
    }).whenComplete(() {
      _apiResolution.complete();
    });
    return _apiResolution.future;
  }

  Future<KSocket> _getSocket(String host, int port) {
    var key = '${host}:${port}';
    if (!_sockets.containsKey(key)) {
      _sockets[key] = KSocket.connect(host, port);
    }

    return _sockets[key];
  }

  Future close() async {
    /// TODO: wait for complition of any in-flight request.
    /// TODO: don't allow any new requests to be send.
    for (Future<KSocket> s in _sockets.values) {
      await (await s).destroy();
    }
    _sockets.clear();
  }
}
