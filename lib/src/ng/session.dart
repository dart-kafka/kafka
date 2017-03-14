import 'dart:async';

import 'package:logging/logging.dart';

import 'io.dart';
import 'versions_api.dart';
import 'versions.dart';

final Logger _logger = new Logger('Session');

/// Contact point used by [Session] to bootstrap connection to Kafka cluster.
class ContactPoint {
  final String host;
  final int port;

  ContactPoint._(this.host, this.port);

  factory ContactPoint(String uri) {
    var u = Uri.parse('kafka://' + uri);
    return new ContactPoint._(u.host, u.port ?? 9092);
  }
}

/// Session responsible for handling connections to all brokers in a Kafka
/// cluster.
///
/// Handles resolution of supported API versions by the server and the client.
/// The latest version supported by both sides is used.
abstract class Session {
  factory Session(List<ContactPoint> contactPoints) {
    return new _SessionImpl(contactPoints);
  }
  List<ContactPoint> get contactPoints;
  Future<T> send<T>(KRequest<T> request, String host, int port);
  Future close();
}

class _SessionImpl implements Session {
  final List<ContactPoint> contactPoints;
  final Map<String, Future<KSocket>> _sockets = new Map();

  /// Resolved API versions supported by both server and client.
  Map<int, int> _apiVersions;
  Completer _apiResolution;

  _SessionImpl(this.contactPoints);

  Future<T> send<T>(KRequest<T> request, String host, int port) {
    Future<KSocket> result;
    if (_apiVersions == null) {
      /// TODO: resolve versions for each node separately
      /// It may not always be true that all nodes in Kafka cluster
      /// support exactly the same versions, e.g. during server upgrades.
      result =
          _resolveApiVersions(host, port).then((_) => _getSocket(host, port));
    } else {
      result = _getSocket(host, port);
    }

    return result.then((socket) {
      var version = _apiVersions[request.apiKey];
      _logger.finest('Sending $request (v$version) to $host:$port');
      var payload = request.encoder.encode(request);
      return socket.sendPacket(request.apiKey, request.apiVersion, payload);
    }).then((responseData) {
      return request.decoder.decode(responseData);
    });
  }

  Future _resolveApiVersions(String host, int port) {
    if (_apiResolution != null) return _apiResolution.future;
    _apiResolution = new Completer();
    var request = new ApiVersionsRequest();
    _getSocket(host, port).then((socket) {
      var payload = request.encoder.encode(request);
      return socket.sendPacket(request.apiKey, request.apiVersion, payload);
    }).then((data) {
      var response = request.decoder.decode(data);
      _apiVersions = resolveApiVersions(response.versions);
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
    for (Future<KSocket> s in _sockets.values) {
      await (await s).destroy();
    }
    _sockets.clear();
  }
}
