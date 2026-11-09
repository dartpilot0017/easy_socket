@TestOn('vm')
library;

import 'dart:async';

import 'package:easy_socket/easy_socket.dart';
import 'package:test/test.dart';

import 'support/test_server.dart';

void main() {
  late TestServer server;
  late EasySocket socket;

  setUp(() async {
    server = TestServer();
    await server.start();
    socket = EasySocket(
      server.url,
      minReconnectDelay: const Duration(milliseconds: 20),
      maxReconnectDelay: const Duration(milliseconds: 50),
    );
  });

  tearDown(() async {
    await socket.dispose();
    await server.stop();
  });

  Future<void> connected() =>
      socket.onStateChange.firstWhere((s) => s == SocketState.connected);

  test('on() connects lazily and routes by event name', () async {
    expect(socket.state, SocketState.idle);
    final chat = Completer<Object?>();
    socket.on('chat', chat.complete);
    socket.on('other', (_) => fail('wrong handler'));
    await connected();

    server.broadcast({'event': 'chat', 'data': 'hi'});
    expect(await chat.future, 'hi');
  });

  test('emit() before connecting is queued and delivered', () async {
    socket.emit('hello', {'n': 1});
    expect(await server.received.stream.first, {
      'event': 'hello',
      'data': {'n': 1},
    });
  });

  test('emit() before connecting waits for the server format', () async {
    server.greeting = {'type': 'welcome', 'payload': null};
    socket.emit('hello', 1);
    socket.send('"raw"');
    final got = server.received.stream.take(2).toList();
    expect(await got, [
      {'type': 'hello', 'payload': 1},
      'raw',
    ]);
  });

  test('emit() mirrors the server format', () async {
    final got = Completer<void>();
    socket.on('ping', (_) => got.complete());
    await connected();
    server.broadcast({'type': 'ping', 'payload': null});
    await got.future;

    socket.emit('pong', 1);
    expect(await server.received.stream.first, {'type': 'pong', 'payload': 1});
  });

  test('stream() delivers payloads', () async {
    final first = socket.stream('tick').first;
    await connected();
    server.broadcast(['tick', 42]);
    expect(await first, 42);
  });

  test('once() fires a single time', () async {
    var count = 0;
    socket.once('x', (_) => count++);
    final done = Completer<void>();
    socket.on('done', (_) => done.complete());
    await connected();
    server
      ..broadcast({'event': 'x'})
      ..broadcast({'event': 'x'})
      ..broadcast({'event': 'done'});
    await done.future;
    expect(count, 1);
  });

  test('reconnects after a drop and keeps listeners', () async {
    final events = <Object?>[];
    socket.on('n', events.add);
    await connected();
    server.broadcast({'event': 'n', 'data': 1});

    final states = <SocketState>[];
    socket.onStateChange.listen(states.add);
    await server.dropClients();
    await connected();

    final got = Completer<void>();
    socket.on('n', (_) => got.complete());
    server.broadcast({'event': 'n', 'data': 2});
    await got.future;

    expect(server.connections, 2);
    expect(states, contains(SocketState.reconnecting));
    expect(events, [1, 2]);
  });

  test('close() stops reconnecting', () async {
    await socket.connect();
    await socket.close();
    expect(socket.state, SocketState.closed);
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(socket.state, SocketState.closed);
    expect(server.connections, 1);
  });

  test('listener exceptions go to onError, not the socket', () async {
    final errors = <Object>[];
    socket.onError(errors.add);
    socket.on('boom', (_) => throw StateError('bad handler'));
    final after = Completer<void>();
    socket.on('after', (_) => after.complete());
    await connected();

    server
      ..broadcast({'event': 'boom'})
      ..broadcast({'event': 'after'});
    await after.future;
    expect(errors.single, isStateError);
    expect(socket.isConnected, isTrue);
  });

  test('gives up after maxReconnectAttempts', () async {
    final port = server.port;
    await server.stop();
    final s = EasySocket(
      'ws://localhost:$port',
      minReconnectDelay: const Duration(milliseconds: 5),
      maxReconnectAttempts: 2,
    );
    await expectLater(s.connect(), throwsStateError);
    expect(s.state, SocketState.closed);
    await s.dispose();
    await server.start(); // so tearDown can stop it
  });

  test('on() and emit() reopen the socket after close()', () async {
    await socket.connect();
    await socket.close();

    final got = server.received.stream.first;
    socket.emit('hi', {'n': 1});
    expect(await got, {
      'event': 'hi',
      'data': {'n': 1},
    });
    expect(server.connections, 2);

    await socket.close();
    socket.on('x', (_) {});
    await connected();
    expect(server.connections, 3);
  });

  test('off() removes a once() listener', () async {
    var calls = 0;
    void handler(Object? _) => calls++;
    socket.once('x', handler);
    socket.off('x', handler);
    final after = Completer<void>();
    socket.on('after', (_) => after.complete());
    await connected();

    server
      ..broadcast({'event': 'x'})
      ..broadcast({'event': 'after'});
    await after.future;
    expect(calls, 0);
  });

  test('the function on() returns removes only that registration', () async {
    var calls = 0;
    void handler(Object? _) => calls++;
    final first = socket.on('x', handler);
    socket.on('x', handler);
    first();
    final after = Completer<void>();
    socket.on('after', (_) => after.complete());
    await connected();

    server
      ..broadcast({'event': 'x'})
      ..broadcast({'event': 'after'});
    await after.future;
    expect(calls, 1);
  });

  test('stops reconnecting when the server rejects the client', () async {
    final errors = <Object>[];
    socket.onError(errors.add);
    await socket.connect();
    final closed = socket.onStateChange.firstWhere(
      (s) => s == SocketState.closed,
    );
    await server.dropClients(1008, 'bad token');
    await closed;

    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(socket.state, SocketState.closed);
    expect(server.connections, 1);
    final error = errors.single as SocketRejectedException;
    expect(error.code, 1008);
    expect(error.reason, 'bad token');
  });

  test('shouldReconnect decides which close codes to retry', () async {
    final s = EasySocket(
      server.url,
      minReconnectDelay: const Duration(milliseconds: 5),
      shouldReconnect: (code, reason) => code != 4000,
    );
    await s.connect();
    final closed = s.onStateChange.firstWhere((x) => x == SocketState.closed);
    await server.dropClients(4000);
    await closed;
    expect(server.connections, 1);
    await s.dispose();
  });

  test(
    'backs off and gives up when the server closes right after accepting',
    () async {
      server.closeOnConnect = 1011;
      final s = EasySocket(
        server.url,
        minReconnectDelay: const Duration(milliseconds: 5),
        maxReconnectDelay: const Duration(milliseconds: 20),
        maxReconnectAttempts: 3,
      );
      s.on('x', (_) {});
      await s.onStateChange
          .firstWhere((x) => x == SocketState.closed)
          .timeout(const Duration(seconds: 3));
      expect(server.connections, 4, reason: 'the first try plus 3 retries');
      await s.dispose();
    },
  );

  group('onConnected', () {
    test('runs before queued messages are sent', () async {
      final s = EasySocket(
        server.url,
        onConnected: (s) => s.emit('hello', 1),
        formatDetectionWindow: Duration.zero,
      );
      final got = server.received.stream.take(2).toList();
      s.emit('queued', 2);
      expect(await got, [
        {'event': 'hello', 'data': 1},
        {'event': 'queued', 'data': 2},
      ]);
      await s.dispose();
    });

    test('runs again on every reconnect', () async {
      var calls = 0;
      final s = EasySocket(
        server.url,
        minReconnectDelay: const Duration(milliseconds: 5),
        onConnected: (_) => calls++,
      );
      await s.connect();
      await server.dropClients();
      await s.onStateChange.firstWhere((x) => x == SocketState.reconnecting);
      await s.onStateChange.firstWhere((x) => x == SocketState.connected);
      expect(calls, 2);
      await s.dispose();
    });

    test('can wait for a server reply before counting as connected', () async {
      server.greeting = {'event': 'welcome', 'data': 'hi'};
      late bool connectedDuringHook;
      final s = EasySocket(
        server.url,
        onConnected: (s) async {
          connectedDuringHook = s.isConnected;
          expect(await s.stream('welcome').first, 'hi');
        },
      );
      await s.connect();
      expect(connectedDuringHook, isFalse);
      expect(s.isConnected, isTrue);
      await s.dispose();
    });

    test('a throwing hook is reported and the socket reconnects', () async {
      final errors = <Object>[];
      var calls = 0;
      final s = EasySocket(
        server.url,
        minReconnectDelay: const Duration(milliseconds: 5),
        onConnected: (_) {
          if (calls++ == 0) throw StateError('handshake failed');
        },
      )..onError(errors.add);
      await s.connect();
      expect(errors.single, isStateError);
      expect(server.connections, 2);
      await s.dispose();
    });
  });

  test('emit after close() during onConnected is queued, not lost', () async {
    final started = Completer<void>();
    final s = EasySocket(
      server.url,
      formatDetectionWindow: Duration.zero,
      onConnected: (s) {
        if (!started.isCompleted) started.complete();
        return Completer<void>().future; // never finishes
      },
    );
    s.connect().ignore();
    await started.future;
    await s.close();
    s.emit('after', 1); // reopens; the hook blocks again, so it stays queued
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(s.state, isNot(SocketState.connected));
    await s.dispose();
  });

  group('auth', () {
    test('queryParameters are fetched again for every connection', () async {
      var n = 0;
      final s = EasySocket(
        '${server.url}/ws?room=a',
        minReconnectDelay: const Duration(milliseconds: 5),
        queryParameters: () async => {'token': 't${n++}'},
      );
      await s.connect();
      await server.dropClients();
      await s.onStateChange.firstWhere((x) => x == SocketState.reconnecting);
      await s.onStateChange.firstWhere((x) => x == SocketState.connected);
      expect(server.requests.map((r) => r.$1.toString()), [
        '/ws?room=a&token=t0',
        '/ws?room=a&token=t1',
      ]);
      await s.dispose();
    });

    test('headers are sent on the upgrade request', () async {
      final s = EasySocket(
        server.url,
        headers: () => {'Authorization': 'Bearer abc'},
      );
      await s.connect();
      expect(server.requests.single.$2, 'Bearer abc');
      await s.dispose();
    });
  });

  group('heartbeat', () {
    EasySocket pinging() => EasySocket(
      server.url,
      minReconnectDelay: const Duration(milliseconds: 5),
      pingInterval: const Duration(milliseconds: 40),
      pingMessage: {'event': 'ping'},
    );

    test('keeps a responsive connection open', () async {
      server.replyToPing = true;
      final s = pinging();
      await s.connect();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(s.isConnected, isTrue);
      expect(server.connections, 1);
      await s.dispose();
    });

    test('reconnects when the server stops answering', () async {
      final errors = <Object>[];
      final s = pinging()..onError(errors.add);
      await s.connect();
      await s.onStateChange.firstWhere((x) => x == SocketState.reconnecting);
      await s.onStateChange.firstWhere((x) => x == SocketState.connected);
      expect(server.connections, greaterThanOrEqualTo(2));
      expect(errors.first, isA<TimeoutException>());
      await s.dispose();
    });
  });

  test('connectTimeout gives up on a server that never answers', () async {
    server.stall = true;
    final s = EasySocket(
      server.url,
      connectTimeout: const Duration(milliseconds: 100),
      maxReconnectAttempts: 0,
    );
    await expectLater(
      s.connect().timeout(const Duration(seconds: 2)),
      throwsStateError,
    );
    await s.dispose();
  });

  test('a full offline queue reports the dropped message', () async {
    final errors = <Object>[];
    final s = EasySocket(server.url, maxQueuedMessages: 2)..onError(errors.add);
    s
      ..emit('a')
      ..emit('b')
      ..emit('c');
    final dropped = errors.single as MessageDroppedException;
    expect(dropped.event, 'a');
    await s.dispose();
  });

  test('a state listener may change the state while being notified', () async {
    final states = <SocketState>[];
    socket.onStateChange.listen((s) {
      states.add(s);
      if (s == SocketState.connected) socket.close();
    });
    await socket.connect();
    expect(socket.state, SocketState.closed);
    expect(states, [
      SocketState.connecting,
      SocketState.connected,
      SocketState.closed,
    ]);
  });

  test('connect() during the retry wait reconnects right away', () async {
    final s = EasySocket(
      server.url,
      minReconnectDelay: const Duration(seconds: 30),
      maxReconnectDelay: const Duration(seconds: 30),
    );
    await s.connect();
    final waiting = s.onStateChange.firstWhere(
      (x) => x == SocketState.reconnecting,
    );
    await server.dropClients();
    await waiting;
    await s.connect().timeout(const Duration(seconds: 2));
    expect(server.connections, 2);
    await s.dispose();
  });

  test('debug logs hide auth values', () async {
    final logs = <String>[];
    final s = EasySocket(
      '${server.url}/ws?room=lobby&api_key=url-secret-1',
      debug: true,
      logger: logs.add,
      queryParameters: () => {'token': 'query/secret+2'},
      headers: () => {'Authorization': 'Bearer header-secret-3'},
    );
    await s.connect();
    final all = logs.join('\n');
    expect(all, contains('room=lobby'));
    for (final secret in [
      'url-secret-1',
      'query/secret+2',
      'header-secret-3',
    ]) {
      expect(all, isNot(contains(secret)));
      expect(all, isNot(contains(Uri.encodeQueryComponent(secret))));
    }
    expect(all, contains('token=***'));
    await s.dispose();
  });
}
