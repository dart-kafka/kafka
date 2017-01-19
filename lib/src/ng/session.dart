import 'dart:async';

import 'package:logging/logging.dart';

import 'io.dart';

class KSession {
  static final Logger _logger = new Logger('KSession');
  
  final Map<String, Future<KSocket>> _sockets = new Map();

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
