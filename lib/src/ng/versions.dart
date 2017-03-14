import 'dart:math';
import 'common.dart';
import 'versions_api.dart';

const List<ApiVersion> supportedVersions = const [
  const ApiVersion(ApiKeys.produce, 2, 2),
  const ApiVersion(ApiKeys.fetch, 2, 2),
  const ApiVersion(ApiKeys.offsets, 1, 1),
  const ApiVersion(ApiKeys.metadata, 0, 0),
  const ApiVersion(ApiKeys.offsetCommit, 2, 2),
  const ApiVersion(ApiKeys.offsetFetch, 1, 1),
  const ApiVersion(ApiKeys.groupCoordinator, 0, 0),
  const ApiVersion(ApiKeys.joinGroup, 1, 1),
  const ApiVersion(ApiKeys.heartbeat, 0, 0),
  const ApiVersion(ApiKeys.leaveGroup, 0, 0),
  const ApiVersion(ApiKeys.syncGroup, 0, 0),
  const ApiVersion(ApiKeys.apiVersions, 0, 0),
];

Map<int, int> resolveApiVersions(List<ApiVersion> serverVersions) {
  Map<int, ApiVersion> serverMap =
      new Map.fromIterable(serverVersions, key: (ApiVersion v) => v.key);
  Map<int, ApiVersion> clientMap =
      new Map.fromIterable(serverVersions, key: (ApiVersion v) => v.key);
  Map<int, int> result = new Map();
  for (var key in clientMap.keys) {
    var client = clientMap[key];
    var server = serverMap[key];

    /// Check if version ranges overlap. If they don't then client
    /// can't communicate with this Kafka broker.
    if (!overlap(client.min, client.max, server.min, server.max)) {
      throw 'Unsupported API version: $server. Client supported versions are: $client.';
    }

    /// Find latest possible version supported by both client and server.
    var version = client.max;
    while (version > server.max) {
      version--;
    }
    result[key] = version;
  }
  return result;
}

/// Checks if two integer ranges overlap.
bool overlap(int x1, int x2, int y1, y2) {
  return x2 >= y1 && y2 >= x1;
}
