import 'package:string_scanner/string_scanner.dart';
import 'package:source_span/source_span.dart';

class BnfScanner {
  final SpanScanner _scanner;

  BnfScanner(String source) : _scanner = new SpanScanner.eager(source);

  scan() {
    _scanner.scan('A-Za-z');
  }
}

Packet packetDecode(List<int> bytes) {}

class Packet {
  final int size;
  final List<int> payload;

  Packet(this.size, this.payload);
}

class Payload {
  factory Payload.decodeRequest() {
    return null;
  }

  static List<int> encodeRequest(Request request) {
    return null;
  }
}

class Request implements Payload {}
