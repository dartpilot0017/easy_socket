// Checks how a real server and its proxies treat an easy_socket connection:
// how long connecting takes, and whether anything closes an idle socket.
//
//   dart run example/probe.dart wss://api.example.com/ws/ --idle 120
//   dart run example/probe.dart wss://... --ping 0 --query token=abc
//   dart run example/probe.dart wss://... --header "Authorization: Bearer abc"
//
// --ping 0 turns the heartbeat off, to see the proxy's raw idle timeout.
// Run it once with --ping 0 and once with the default 25 to compare.
import 'dart:async';
import 'dart:io';

import 'package:easy_socket/easy_socket.dart';

Future<void> main(List<String> args) async {
  if (args.isEmpty || args.first.startsWith('-')) {
    stderr.writeln(
      'Usage: dart run example/probe.dart <ws-url> [--idle seconds] '
      '[--ping seconds] [--query k=v] [--header "Name: value"]',
    );
    exit(64);
  }
  final url = args.first;
  var idle = 90;
  var ping = 25;
  final query = <String, String>{};
  final headers = <String, String>{};
  for (var i = 1; i < args.length - 1; i += 2) {
    final value = args[i + 1];
    switch (args[i]) {
      case '--idle':
        idle = int.parse(value);
      case '--ping':
        ping = int.parse(value);
      case '--query':
        final at = value.indexOf('=');
        query[value.substring(0, at)] = value.substring(at + 1);
      case '--header':
        final at = value.indexOf(':');
        headers[value.substring(0, at).trim()] = value.substring(at + 1).trim();
    }
  }

  final clock = Stopwatch()..start();
  String t() => '${(clock.elapsedMilliseconds / 1000).toStringAsFixed(1)}s';
  final drops = <String>[];
  var frames = 0;

  final socket = EasySocket(
    url,
    pingInterval: ping == 0 ? null : Duration(seconds: ping),
    queryParameters: query.isEmpty ? null : () => query,
    headers: headers.isEmpty ? null : () => headers,
  );
  socket.onAny((event) {
    frames++;
    print('[${t()}] ← ${event.name}');
  });
  socket.onError((e) => print('[${t()}] error: $e'));
  socket.onStateChange.listen((state) {
    print('[${t()}] ${state.name}');
    if (state == SocketState.reconnecting || state == SocketState.closed) {
      if (socket.closeCode != null || state == SocketState.reconnecting) {
        drops.add(
          '${t()} (code ${socket.closeCode}'
          '${socket.closeReason?.isNotEmpty == true ? ', ${socket.closeReason}' : ''})',
        );
      }
    }
  });

  try {
    await socket.connect().timeout(const Duration(seconds: 15));
  } catch (e) {
    print('Could not connect: $e');
    await socket.dispose();
    exit(1);
  }
  print(
    'Connected in ${t()}. Staying idle for ${idle}s '
    '(heartbeat ${ping == 0 ? 'off' : 'every ${ping}s'})…',
  );
  await Future<void>.delayed(Duration(seconds: idle));

  print('\n--- result ---');
  print('Frames received: $frames');
  if (drops.isEmpty) {
    print('No disconnects in ${idle}s. Idle connections survive.');
  } else {
    print('Disconnected ${drops.length} time(s), at: ${drops.join('; ')}');
    if (ping == 0) {
      print(
        'Something closes idle sockets. Keep pingInterval shorter than '
        'the first drop time.',
      );
    }
  }
  await socket.dispose();
  exit(0);
}
