import 'errors.dart';
import 'io.dart';

class ApiVersionsRequest implements KRequest<ApiVersionsResponse> {
  @override
  final int apiKey = 18;

  @override
  final int apiVersion = 0;

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
}

class ApiVersion {
  final int key;
  final int min;
  final int max;

  ApiVersion(this.key, this.min, this.max);

  @override
  String toString() => 'ApiVersion{$key, min: $min, max: $max}';
}

class _ApiVersionsRequestEncoder implements RequestEncoder<ApiVersionsRequest> {
  const _ApiVersionsRequestEncoder();

  @override
  List<int> encode(ApiVersionsRequest request) {
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
