import 'dart:async';

import 'package:logging/logging.dart';

import 'io.dart';

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

  _SessionImpl(this.contactPoints);

  Future<T> send<T>(KRequest<T> request, String host, int port) {
    _logger.finest('Sending $request to $host:$port');
    var payload = request.encoder.encode(request);
    return _getSocket(host, port).then((socket) {
      return socket.sendPacket(request.apiKey, request.apiVersion, payload);
    }).then((responseData) {
      return request.decoder.decode(responseData);
    });
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
