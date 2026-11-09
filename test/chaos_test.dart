@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';

import 'package:easy_socket/easy_socket.dart';
import 'package:test/test.dart';

import 'support/test_server.dart';

/// Unfriendly networks and unusual use: restarts, floods, huge frames and
/// calls at awkward moments.
void main() {
  late TestServer server;
  final sockets = <EasySocket>[];
  final errors = <Object>[];

  EasySocket socket({int maxQueuedMessages = 100}) {
    final s = EasySocket(
      server.url,
      minReconnectDelay: const Duration(milliseconds: 5),
      maxReconnectDelay: const Duration(milliseconds: 20),
      maxQueuedMessages: maxQueuedMessages,
      formatDetectionWindow: Duration.zero,
    )..onError(errors.add);
    sockets.add(s);
    return s;
  }

  Future<void> state(EasySocket s, SocketState target) => s.state == target
      ? Future.value()
      : s.onStateChange
            .firstWhere((x) => x == target)
            .timeout(const Duration(seconds: 5));

  Future<void> until(bool Function() condition) async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (!condition()) {
      if (DateTime.now().isAfter(deadline)) fail('timed out');
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }

  setUp(() async {
    errors.clear();
    server = TestServer();
    await server.start();
  });

  tearDown(() async {
    for (final s in sockets) {
      await s.dispose();
    }
    sockets.clear();
    await server.stop();
  });

  test('survives a server restart in the middle of sending', () async {
    final s = socket(maxQueuedMessages: 1000);
    final got = <int>[];
    server.received.stream.listen((m) => got.add((m as Map)['data'] as int));
    await s.connect();
    final port = server.port;

    for (var i = 0; i < 100; i++) {
      s.emit('n', i);
    }
    await server.stop();
    for (var i = 100; i < 200; i++) {
      s.emit('n', i);
      if (i == 150) {
        server = TestServer();
        await server.start(port);
        server.received.stream.listen(
          (m) => got.add((m as Map)['data'] as int),
        );
      }
    }
    await until(() => got.contains(199));
    await state(s, SocketState.connected);

    // Messages written just as the old connection died can be lost (that is
    // websockets: delivery is at most once), but nothing arrives twice or
    // out of order, and everything sent after the drop gets through.
    expect(got.toSet(), hasLength(got.length), reason: 'no duplicates');
    expect(got, orderedEquals(List.of(got)..sort()));
    expect(got, containsAll(List.generate(100, (i) => 100 + i)));
    expect(
      errors.where((e) => e is! TimeoutException && e is! Exception),
      isEmpty,
    );
  });

  test('50 drops in a row leave exactly one live connection', () async {
    final s = socket();
    await s.connect();
    for (var i = 0; i < 50; i++) {
      final dropped = state(s, SocketState.reconnecting);
      await server.dropClients();
      await dropped;
      await state(s, SocketState.connected);
    }
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.clients, hasLength(1));
    expect(server.connections, 51);
  });

  test('5000 messages each way arrive complete and in order', () async {
    final s = socket();
    final fromServer = <int>[];
    s.on('n', (d) => fromServer.add(d as int));
    final toServer = <int>[];
    server.received.stream.listen(
      (m) => toServer.add((m as Map)['data'] as int),
    );
    await s.connect();

    for (var i = 0; i < 5000; i++) {
      s.emit('n', i);
      server.broadcast({'event': 'n', 'data': i});
    }
    await until(() => fromServer.length == 5000 && toServer.length == 5000);
    expect(fromServer, List.generate(5000, (i) => i));
    expect(toServer, List.generate(5000, (i) => i));
  });

  test('handles a 5 MB message each way', () async {
    final big = 'x' * (5 * 1024 * 1024);
    final s = socket();
    final got = Completer<Object?>();
    s.on('big', got.complete);
    await s.connect();

    final received = server.received.stream.first;
    s.emit('big', big);
    server.broadcast({'event': 'big', 'data': big});
    expect(((await received) as Map)['data'], hasLength(big.length));
    expect(await got.future, hasLength(big.length));
  });

  test('dispose() from inside a handler is safe', () async {
    final s = socket();
    var calls = 0;
    s.on('stop', (_) {
      calls++;
      s.dispose();
    });
    await s.connect();
    server
      ..broadcast({'event': 'stop'})
      ..broadcast({'event': 'stop'});
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(calls, 1);
    expect(s.state, SocketState.closed);
    expect(() => s.emit('x'), throwsStateError);
    await until(() => server.clients.isEmpty);
  });

  test('connect() while reconnecting waits without opening twice', () async {
    final s = socket();
    await s.connect();
    final port = server.port;
    await server.stop();
    await state(s, SocketState.reconnecting);

    final waiting = [s.connect(), s.connect()];
    server = TestServer();
    await server.start(port);
    await Future.wait(waiting).timeout(const Duration(seconds: 5));
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.connections, 1);
  });

  test('close() while the connection is opening leaks nothing', () async {
    final s = socket();
    final opening = expectLater(s.connect(), throwsStateError);
    await s.close();
    await opening;
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(server.clients, isEmpty);
    expect(s.state, SocketState.closed);
  });

  test('odd frames from the server never break the socket', () async {
    final s = socket();
    final any = <SocketEvent>[];
    s.onAny(any.add);
    await s.connect();
    for (final frame in [
      'not json',
      '{"broken": ',
      '42',
      'null',
      '[]',
      '[1, 2]',
      '{}',
      '{"event": 5}',
      jsonEncode({'event': 'ok', 'data': 1}),
    ]) {
      server.broadcast(frame);
    }
    for (final c in server.clients) {
      c.add([0, 159, 146, 150]); // binary
    }
    await until(() => any.any((e) => e.name == 'ok'));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(s.isConnected, isTrue);
    expect(any.where((e) => e.name == 'ok').single.data, 1);
    expect(errors, isEmpty);
  });
}
