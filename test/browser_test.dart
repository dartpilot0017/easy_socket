@TestOn('browser')
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:easy_socket/easy_socket.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:test/test.dart';

/// Drives the VM-side server in support/hybrid_server.dart.
class Remote {
  Remote._(this._channel, this._events, this.port);

  static Future<Remote> start() async {
    final channel = spawnHybridUri('support/hybrid_server.dart');
    final events = channel.stream.asBroadcastStream();
    final hello = await events.first as Map;
    return Remote._(channel, events, hello['port'] as int);
  }

  final StreamChannel<Object?> _channel;
  final Stream<Object?> _events;
  final int port;

  String get url => 'ws://127.0.0.1:$port';

  /// Frames the server received from clients.
  Stream<Map> get received =>
      _events.cast<Map>().where((e) => e.containsKey('received'));

  Future<Map> command(String cmd, [Map<String, Object?> args = const {}]) {
    final reply = _events.cast<Map>().firstWhere((e) => e['done'] == cmd);
    _channel.sink.add({'cmd': cmd, ...args});
    return reply;
  }

  Future<int> connections() async =>
      (await command('stats'))['connections'] as int;

  void stop() => _channel.sink.close();
}

void main() {
  late Remote server;
  final sockets = <EasySocket>[];

  EasySocket socket({
    String? url,
    Duration? pingInterval,
    Object? pingMessage,
    FutureOr<Map<String, String>> Function()? queryParameters,
    FutureOr<Map<String, String>> Function()? headers,
  }) {
    final s = EasySocket(
      url ?? server.url,
      minReconnectDelay: const Duration(milliseconds: 20),
      maxReconnectDelay: const Duration(milliseconds: 50),
      pingInterval: pingInterval,
      pingMessage: pingMessage,
      queryParameters: queryParameters,
      headers: headers,
    );
    sockets.add(s);
    return s;
  }

  Future<void> state(EasySocket s, SocketState target) => s.state == target
      ? Future.value()
      : s.onStateChange.firstWhere((x) => x == target);

  setUp(() async => server = await Remote.start());
  tearDown(() async {
    for (final s in sockets) {
      await s.dispose();
    }
    sockets.clear();
    server.stop();
  });

  test('routes events and mirrors the server format', () async {
    await server.command('set', {
      'greeting': {'type': 'hello', 'payload': 1},
    });
    final s = socket();
    final hello = Completer<Object?>();
    s.on('hello', hello.complete);
    expect(await hello.future, 1);

    final got = server.received.first;
    s.emit('chat', {'text': 'hi'});
    expect((await got)['received'], {
      'type': 'chat',
      'payload': {'text': 'hi'},
    });
  });

  test('queues while connecting and reconnects after a drop', () async {
    final s = socket();
    final got = server.received.first;
    s.emit('early', 1);
    expect((await got)['received'], {'event': 'early', 'data': 1});

    final events = <Object?>[];
    s.on('n', events.add);
    await server.command('drop');
    await state(s, SocketState.reconnecting);
    await state(s, SocketState.connected);
    await server.command('broadcast', {
      'message': {'event': 'n', 'data': 2},
    });
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(events, [2]);
    expect(await server.connections(), 2);
  });

  test('close() then emit() reopens', () async {
    final s = socket();
    await s.connect();
    await s.close();
    final got = server.received.first;
    s.emit('again');
    expect((await got)['received'], {'event': 'again'});
  });

  test('a refusing close code stops retrying, with code and reason', () async {
    final errors = <Object>[];
    final s = socket()..onError(errors.add);
    await s.connect();
    await server.command('drop', {'code': 1008, 'reason': 'bad token'});
    await state(s, SocketState.closed);
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(await server.connections(), 1);
    final error = errors.whereType<SocketRejectedException>().single;
    expect(error.code, 1008);
    expect(error.reason, 'bad token');
  });

  test('queryParameters reach the server', () async {
    final s = socket(
      url: '${server.url}/ws?room=a',
      queryParameters: () async => {'token': 'abc'},
    );
    await s.connect();
    expect((await server.command('stats'))['requests'], [
      '/ws?room=a&token=abc',
    ]);
  });

  test('headers fail clearly instead of retrying', () async {
    final errors = <Object>[];
    final s = socket(headers: () => {'Authorization': 'x'})
      ..onError(errors.add);
    await expectLater(s.connect(), throwsUnsupportedError);
    expect(s.state, SocketState.closed);
    expect(errors.single, isUnsupportedError);
  });

  test('pingMessage keeps a live connection and replaces a dead one', () async {
    await server.command('set', {'replyToPing': true});
    final s = socket(
      pingInterval: const Duration(milliseconds: 50),
      pingMessage: {'event': 'ping'},
    );
    await s.connect();
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(s.isConnected, isTrue);
    expect(await server.connections(), 1);

    await server.command('set', {'replyToPing': false});
    await state(s, SocketState.reconnecting);
    await state(s, SocketState.connected);
    expect(await server.connections(), greaterThanOrEqualTo(2));
  });

  test('sends binary frames', () async {
    final s = socket();
    await s.connect();
    final got = server.received.first;
    s.send(Uint8List.fromList([1, 2, 3]));
    final frame = await got;
    expect(frame['binary'], isTrue);
    expect(frame['received'], [1, 2, 3]);
  });

  test(
    'gives up on an unreachable server after maxReconnectAttempts',
    () async {
      final s = EasySocket(
        'ws://127.0.0.1:1',
        minReconnectDelay: const Duration(milliseconds: 5),
        maxReconnectAttempts: 1,
      );
      sockets.add(s);
      await expectLater(s.connect(), throwsStateError);
    },
  );
}
