// Live demo: starts a local chat server and connects to it with EasySocket.
//
//   dart run example/live_demo.dart
//
// The server speaks {"type": ..., "payload": ...}, a different format from
// easy_socket's default, and drops the connection partway through to show
// auto-reconnect.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:easy_socket/easy_socket.dart';

Future<void> main() async {
  final server = await startServer();
  final url = 'ws://localhost:${server.port}';
  print('server listening on $url\n');

  // ---- This is all a developer writes ----
  final socket = EasySocket(url);

  socket.onStateChange.listen((s) => print('  [state] ${s.name}'));
  socket.on('welcome', (data) => print('  welcome: $data'));
  socket.on('message.created', (data) => print('  new message: $data'));

  socket.emit('chat.send', {'text': 'sent before the socket even connected'});
  // -----------------------------------------

  await Future<void>.delayed(const Duration(milliseconds: 500));
  socket.emit('chat.send', {'text': 'second message'});

  await Future<void>.delayed(const Duration(milliseconds: 500));
  print('\n-- server drops every client --');
  await server.dropAll();

  await Future<void>.delayed(const Duration(seconds: 2));
  socket.emit('chat.send', {'text': 'after reconnecting'});

  await Future<void>.delayed(const Duration(milliseconds: 500));
  await socket.dispose();
  await server.close();
  print('\ndone');
}

class DemoServer {
  DemoServer(this._http);
  final HttpServer _http;
  final _clients = <WebSocket>[];

  int get port => _http.port;

  Future<void> dropAll() async {
    for (final c in List.of(_clients)) {
      await c.close();
    }
  }

  Future<void> close() async {
    await dropAll();
    await _http.close(force: true);
  }
}

Future<DemoServer> startServer() async {
  final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final server = DemoServer(http);
  http.listen((req) async {
    final ws = await WebSocketTransformer.upgrade(req);
    server._clients.add(ws);
    ws.add(
      jsonEncode({
        'type': 'welcome',
        'payload': 'hello client #${server._clients.length}',
      }),
    );
    ws.listen((frame) {
      print('  (server got: $frame)');
      final msg = jsonDecode(frame as String) as Map<String, dynamic>;
      if (msg['type'] == 'chat.send') {
        ws.add(
          jsonEncode({'type': 'message.created', 'payload': msg['payload']}),
        );
      }
    }, onDone: () => server._clients.remove(ws));
  });
  return server;
}
