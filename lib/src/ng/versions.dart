import 'versions_api.dart';
import 'common.dart';

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
