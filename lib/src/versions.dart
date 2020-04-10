import 'common.dart';
import 'versions_api.dart';

export 'common.dart' show ApiKey;
export 'versions_api.dart' show ApiVersion;

const List<ApiVersion> supportedVersions = const [
  const ApiVersion(ApiKey.produce, 2, 2),
  const ApiVersion(ApiKey.fetch, 2, 2),
  const ApiVersion(ApiKey.offsets, 1, 1),
  const ApiVersion(ApiKey.metadata, 0, 0),
  const ApiVersion(ApiKey.offsetCommit, 2, 2),
  const ApiVersion(ApiKey.offsetFetch, 1, 1),
  const ApiVersion(ApiKey.groupCoordinator, 0, 0),
  const ApiVersion(ApiKey.joinGroup, 1, 1),
  const ApiVersion(ApiKey.heartbeat, 0, 0),
  const ApiVersion(ApiKey.leaveGroup, 0, 0),
  const ApiVersion(ApiKey.syncGroup, 0, 0),
  const ApiVersion(ApiKey.apiVersions, 0, 0),
];

/// Returns a map where keys are one of [ApiKey] constants and values
/// contain version number of corresponding Kafka API, which:
///
/// - supported by both server and client
/// - is highest possible.
///
/// This function throws `UnsupportedError` if any of API versions
/// between server and client don't have an overlap.
Map<int, int> resolveApiVersions(
    List<ApiVersion> serverVersions, List<ApiVersion> clientVersions) {
  Map<int, ApiVersion> serverMap =
      new Map.fromIterable(serverVersions, key: (v) => v.key);
  Map<int, ApiVersion> clientMap =
      new Map.fromIterable(clientVersions, key: (v) => v.key);
  Map<int, int> result = new Map();
  for (var key in clientMap.keys) {
    var client = clientMap[key];
    var server = serverMap[key];

    /// Check if version ranges overlap. If they don't then client
    /// can't communicate with this Kafka broker.
    if (!overlap(client.min, client.max, server.min, server.max)) {
      throw new UnsupportedError(
          'Unsupported API version: $server. Client supported versions are: $client.');
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
