import 'dart:io';
import 'dart:async';

/// Returns default host's IP address depending on current environment.
///
/// For running tests locally on developer machine we assume you're using
/// Docker Toolbox and OS X (sorry). The IP of `default` docker-machine will
/// be used.
Future<String> getDefaultHost() async {
  Platform.environment.forEach((k, v) => print('$k : $v'));
  if (Platform.environment.containsKey('TRAVIS') &&
      Platform.environment['TRAVIS'] == true) {
    return '127.0.0.1';
  } else {
    var res = await Process.run('docker-machine', ['ip', 'default']);
    return res.stdout.toString().trim();
  }
}
