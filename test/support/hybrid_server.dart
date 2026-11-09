// A websocket server that runs on the VM and is driven by browser tests
// through spawnHybridUri.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:stream_channel/stream_channel.dart';

Future<void> hybridMain(StreamChannel<Object?> channel) async {
  final clients = <WebSocket>[];
  final requests = <String>[];
  var connections = 0;
  Object? greeting;
  int? closeOnConnect;
  var replyToPing = false;

  final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  http.listen((req) async {
    requests.add(req.uri.toString());
    final ws = await WebSocketTransformer.upgrade(req);
    connections++;
    if (closeOnConnect != null) {
      await ws.close(closeOnConnect);
      return;
    }
    clients.add(ws);
    if (greeting != null) ws.add(jsonEncode(greeting));
    ws.listen((m) {
      final message = m is String ? jsonDecode(m) : m;
      if (replyToPing && message is Map && message['event'] == 'ping') {
        ws.add(jsonEncode({'event': 'pong'}));
        return;
      }
      channel.sink.add({'received': message, 'binary': m is! String});
    }, onDone: () => clients.remove(ws));
  });

  channel.sink.add({'port': http.port});
  await for (final command in channel.stream) {
    command as Map;
    switch (command['cmd']) {
      case 'broadcast':
        for (final c in clients) {
          c.add(jsonEncode(command['message']));
        }
      case 'drop':
        for (final c in List.of(clients)) {
          await c.close(command['code'] as int?, command['reason'] as String?);
        }
      case 'set':
        greeting = command['greeting'];
        closeOnConnect = command['closeOnConnect'] as int?;
        replyToPing = command['replyToPing'] == true;
    }
    channel.sink.add({
      'done': command['cmd'],
      'connections': connections,
      'requests': requests,
    });
  }
  await http.close(force: true);
}
