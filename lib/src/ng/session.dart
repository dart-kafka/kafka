import 'dart:async';

import 'package:logging/logging.dart';

import 'io.dart';

final KSession KAFKA_DEFAULT_SESSION = new KSession();
List<ContactPoint> _defaultContactPoints;

kafkaConfigure(List<ContactPoint> defaultContactPoints) {
  _defaultContactPoints = defaultContactPoints;
}

Future kafkaShutdown() {
  return KAFKA_DEFAULT_SESSION.close();
}

class ContactPoint {
  final String host;
  final int port;

  ContactPoint._(this.host, this.port);

  factory ContactPoint(String uri) {
    var u = Uri.parse('kafka://' + uri);
    return new ContactPoint._(u.host, u.port ?? 9092);
  }
}

abstract class KSession {
  factory KSession({List<ContactPoint> contactPoints}) {
    return new _KSessionImpl(contactPoints);
  }
  List<ContactPoint> get contactPoints;
  Future<T> send<T>(KRequest<T> request, String host, int port);
  Future close();
}

class _KSessionImpl implements KSession {
  static final Logger _logger = new Logger('KSession');

  final Map<String, Future<KSocket>> _sockets = new Map();

  _KSessionImpl(this._contactPoints);

  List<ContactPoint> _contactPoints;
  List<ContactPoint> get contactPoints {
    if (_contactPoints == null) {
      _contactPoints = _defaultContactPoints;
    }
    return _contactPoints;
  }

  Future<T> send<T>(KRequest<T> request, String host, int port) {
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
