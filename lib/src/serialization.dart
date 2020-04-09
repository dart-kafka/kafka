import 'dart:convert';

abstract class Serializer<T> {
  List<int> serialize(T data);
}

/// Serializer for `String` objects. Defaults to UTF8 encoding.
class StringSerializer implements Serializer<String> {
  @override
  List<int> serialize(String data) => utf8.encode(data);
}

class CodecSerializer<S> implements Serializer<S> {
  final Codec<S, List<int>> codec;

  CodecSerializer(this.codec);

  @override
  List<int> serialize(S data) {
    return codec.encode(data);
  }
}

abstract class Deserializer<T> {
  T deserialize(List<int> data);
}

/// Deserializer for `String` objects. Defaults to UTF8 encoding.
class StringDeserializer implements Deserializer<String> {
  @override
  String deserialize(List<int> data) => utf8.decode(data);
}
