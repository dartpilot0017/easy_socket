import 'dart:convert';

import 'package:easy_socket/easy_socket.dart';
import 'package:test/test.dart';

void main() {
  late AutoCodec codec;
  setUp(() => codec = AutoCodec());

  group('decode', () {
    test('event/data envelope', () {
      final e = codec.decode('{"event":"chat","data":{"text":"hi"}}');
      expect(e.name, 'chat');
      expect(e.data, {'text': 'hi'});
    });

    test('type/payload envelope', () {
      final e = codec.decode('{"type":"chat","payload":[1,2]}');
      expect(e.name, 'chat');
      expect(e.data, [1, 2]);
    });

    test('flat object passes the rest as data', () {
      final e = codec.decode('{"action":"join","room":"a"}');
      expect(e.name, 'join');
      expect(e.data, {'room': 'a'});
    });

    test('array tuple', () {
      final e = codec.decode('["chat",{"text":"hi"}]');
      expect(e.name, 'chat');
      expect(e.data, {'text': 'hi'});
    });

    test('plain text and unnamed JSON fall back to "message"', () {
      expect(codec.decode('hello').name, SocketEvent.fallbackName);
      expect(codec.decode('hello').data, 'hello');
      expect(codec.decode('{"x":1}').name, SocketEvent.fallbackName);
      expect(codec.decode('{"x":1}').data, {'x': 1});
      expect(codec.decode([1, 2, 3]).data, [1, 2, 3]);
    });

    test('non-string name is ignored', () {
      expect(codec.decode('{"type":5}').name, SocketEvent.fallbackName);
    });
  });

  group('encode', () {
    Object? enc(String e, Object? d) => jsonDecode(codec.encode(e, d));

    test('defaults to event/data', () {
      expect(codec.hasDetectedFormat, isFalse);
      expect(enc('a', 1), {'event': 'a', 'data': 1});
    });

    test('mirrors a detected envelope', () {
      codec.decode('{"type":"x","payload":null}');
      expect(enc('a', 1), {'type': 'a', 'payload': 1});
    });

    test('mirrors a flat format', () {
      codec.decode('{"type":"x","id":1}');
      expect(enc('a', {'id': 2}), {'type': 'a', 'id': 2});
      expect(enc('a', 3), {'type': 'a', 'data': 3});
    });

    test('mirrors arrays', () {
      codec.decode('["x",1]');
      expect(enc('a', 2), ['a', 2]);
    });

    test('keeps the first detected format', () {
      codec.decode('["x",1]');
      codec.decode('{"event":"y","data":1}');
      expect(enc('a', 2), ['a', 2]);
    });
  });

  test('encode leaves out missing data', () {
    final codec = AutoCodec();
    expect(jsonDecode(codec.encode('ping', null)), {'event': 'ping'});
    codec.decode('{"type": "a", "payload": 1}');
    expect(jsonDecode(codec.encode('ping', null)), {'type': 'ping'});
  });
}
