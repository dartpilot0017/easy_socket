import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// A tiny local websocket server for tests.
class TestServer {
  late HttpServer _http;
  final clients = <WebSocket>[];
  final received = StreamController<Object?>.broadcast();
  int connections = 0;
  Object? greeting;

  /// When set, the server accepts each connection and closes it right away
  /// with this code.
  int? closeOnConnect;

  /// Accept the TCP connection but never answer the upgrade.
  bool stall = false;

  /// Answer `{"event": "ping"}` with `{"event": "pong"}`.
  bool replyToPing = false;

  /// The URI and Authorization header of every upgrade request.
  final requests = <(Uri, String?)>[];

  String get url => 'ws://localhost:${_http.port}';

  Future<void> start([int port = 0]) async {
    _http = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
    _http.listen((req) async {
      requests.add((req.uri, req.headers.value('authorization')));
      if (stall) return;
      final ws = await WebSocketTransformer.upgrade(req);
      connections++;
      clients.add(ws);
      if (closeOnConnect != null) {
        await ws.close(closeOnConnect);
        return;
      }
      if (greeting != null) ws.add(jsonEncode(greeting));
      ws.listen((m) {
        final message = jsonDecode(m as String);
        if (replyToPing && message is Map && message['event'] == 'ping') {
          ws.add(jsonEncode({'event': 'pong'}));
          return;
        }
        received.add(message);
      }, onDone: () => clients.remove(ws));
    });
  }

  int get port => _http.port;

  void broadcast(Object message) {
    for (final c in clients) {
      c.add(message is String ? message : jsonEncode(message));
    }
  }

  Future<void> dropClients([int? code, String? reason]) async {
    for (final c in List.of(clients)) {
      await c.close(code, reason);
    }
  }

  Future<void> stop() async {
    await dropClients();
    await _http.close(force: true);
  }
}
