import 'common.dart';
import 'errors.dart';
import 'io.dart';

class ApiVersionsRequest implements KRequest<ApiVersionsResponse> {
  @override
  final int apiKey = ApiKey.apiVersions;

  @override
  ResponseDecoder<ApiVersionsResponse> get decoder =>
      const _ApiVersionsResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _ApiVersionsRequestEncoder();
}

class ApiVersionsResponse {
  final int error;
  final List<ApiVersion> versions;

  ApiVersionsResponse(this.error, this.versions) {
    if (error != Errors.NoError) throw new KafkaError.fromCode(error, this);
  }

  @override
  String toString() => 'ApiVersionsResponse{$versions}';
}

class ApiVersion {
  final int key;
  final int min;
  final int max;

  const ApiVersion(this.key, this.min, this.max);

  @override
  String toString() => 'ApiVersion{$key, min: $min, max: $max}';
}

class _ApiVersionsRequestEncoder implements RequestEncoder<ApiVersionsRequest> {
  const _ApiVersionsRequestEncoder();

  @override
  List<int> encode(ApiVersionsRequest request, int version) {
    assert(version == 0,
        'Only v0 of ApiVersions request is supported by the client.');
    return <int>[];
  }
}

class _ApiVersionsResponseDecoder
    implements ResponseDecoder<ApiVersionsResponse> {
  const _ApiVersionsResponseDecoder();

  @override
  ApiVersionsResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var error = reader.readInt16();
    var versions = reader.readObjectArray<ApiVersion>(
        (_) => new ApiVersion(_.readInt16(), _.readInt16(), _.readInt16()));
    return new ApiVersionsResponse(error, versions);
  }
}
