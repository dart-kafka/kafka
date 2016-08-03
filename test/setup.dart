import 'dart:async';
import 'package:logging/logging.dart';

Future<String> getDefaultHost() async {
  return '127.0.0.1';
}

void enableLogs({Level level: Level.INFO}) {
  Logger.root.level = level;
  Logger.root.onRecord.listen(print);
}
