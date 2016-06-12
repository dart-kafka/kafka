import 'dart:io';
import 'dart:async';
import 'package:logging/logging.dart';

/// Returns default host's IP address depending on current environment.
///
/// For running tests locally on developer machine we assume you're using
/// Docker Toolbox and OS X (sorry). The IP of `default` docker-machine will
/// be used.
Future<String> getDefaultHost() async {
  if (Platform.environment.containsKey('TRAVIS')) {
    return '127.0.0.1';
  } else {
    var res = await Process.run('docker-machine', ['ip', 'default']);
    return res.stdout.toString().trim();
  }
}

void enableLogs() {
  Logger.root.onRecord.listen(print);
}
