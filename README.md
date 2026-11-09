# easy_socket

Websocket events for Dart and Flutter with no setup.

```dart
final socket = EasySocket('wss://api.example.com/ws');

socket.on('message.created', (data) => print(data));
socket.emit('chat.send', {'text': 'hello'});
```

There's no `connect()`, `jsonDecode` or reconnect loop to write. It works on
mobile, desktop and web.

## What it handles for you

- **Lazy connect.** The first `on`, `emit` or `stream` call opens the connection.
- **Auto-reconnect** with exponential backoff and jitter. Listeners survive reconnects.
- **Format auto-detection.** Incoming messages can use any of these shapes:
  - `{"event"|"type"|"action": "name", "data"|"payload"|"body": ...}`
  - flat objects: `{"type": "chat", "text": "hi"}` (the rest of the object is the payload)
  - arrays: `["chat", {...}]`
  - plain text or unrecognised JSON, delivered as a `message` event

  `emit` replies in the same shape the server uses. Events emitted right
  after connecting wait briefly (`formatDetectionWindow`, default 300ms) for
  the server's first message. If nothing arrives in that time, they're sent
  as `{"event": ..., "data": ...}`.
- **Offline queue.** Events emitted while disconnected are sent once the socket connects.
- **Safe listeners.** An exception in one handler goes to `onError` instead of breaking the socket.

## API

```dart
final stop = socket.on('event', (data) {});  // call stop() to remove
socket.once('ready', (data) {});
socket.off('event');
socket.stream('tick').listen(print);
socket.onAny((event) => print('${event.name}: ${event.data}'));
socket.onError(print);

socket.emit('name', data);   // auto-formatted
socket.send('raw frame');    // sent as-is

socket.state;                // idle, connecting, connected, reconnecting, closed
socket.onStateChange.listen(print);

await socket.connect();      // optional
await socket.close();        // keeps listeners; on/once/emit/send/connect reopen it
await socket.dispose();
```

## Auth and handshakes

Tokens are fetched again before every connection, so a reconnect uses a fresh
one. `onConnected` runs after each connection opens, before anything queued is
sent and before the socket counts as connected:

```dart
final socket = EasySocket(
  'wss://api.example.com/ws',
  queryParameters: () async => {'token': await auth.freshToken()},
  onConnected: (socket) async {
    socket.emit('subscribe', {'room': 'general'});
    await socket.stream('subscribed').first; // optional: wait for the server
  },
);
```

If `onConnected` throws or takes longer than `connectTimeout`, the error goes
to `onError` and the socket reconnects.

## Dead connections

After a network change or a sleeping laptop, a connection can die without
closing. easy_socket sends a websocket ping every 25 seconds and reconnects if
the server doesn't answer. Every standard server answers these automatically.

Browsers can't send protocol pings. On the web, set a `pingMessage` your
server replies to; if nothing at all arrives before the next ping is due, the
socket reconnects:

```dart
EasySocket(url, pingMessage: {'event': 'ping'});
```

## Mobile apps

easy_socket is plain Dart, so it doesn't see the app lifecycle or the
network. Two small hooks make reconnects instant instead of waiting for a
heartbeat or a retry delay.

When the app goes to the background, iOS suspends it and the connection
dies. Close it yourself and reopen on return. Listeners and queued messages
are kept:

```dart
final lifecycle = AppLifecycleListener(
  onHide: socket.close,
  onShow: socket.connect,
);
```

When the network comes back, `connect()` skips the remaining retry delay
(here with `connectivity_plus`):

```dart
Connectivity().onConnectivityChanged.listen((results) {
  if (!results.contains(ConnectivityResult.none)) socket.connect();
});
```

The offline queue is in memory, so messages still queued when the app is
killed are lost. For messages that must survive that, keep your own
persisted outbox and send from it once the socket is connected.

Before shipping, check your real server and any proxy in front of it:

```bash
dart run example/probe.dart wss://your.server/ws/ --ping 0 --idle 120
```

It reports whether anything closes idle connections and after how long.
Keep `pingInterval` shorter than that.

## Delivery

Messages sent while disconnected are queued and sent in order once the
connection is back. Like any websocket, delivery is at most once: a message
written just as a connection dies can be lost, and nothing is ever sent
twice. If a message must arrive, have the server acknowledge it (the
`easy_chat` example matches replies by a client id).

## Errors you might see in `onError`

- `SocketRejectedException`: the server closed with a "refused" code, so the
  socket stopped retrying. Using it again reopens it.
- `MessageDroppedException`: the offline queue was full; the oldest message
  was dropped.
- `TimeoutException`: connecting, `onConnected`, or a heartbeat took too long.
  The socket is already reconnecting.
- Anything a listener throws.

## Options

```dart
EasySocket(
  url,
  autoReconnect: true,
  minReconnectDelay: Duration(milliseconds: 500),
  maxReconnectDelay: Duration(seconds: 30),
  maxReconnectAttempts: null,   // null = retry forever
  // Stops retrying when the server refuses the client (close codes 1002,
  // 1003, 1007, 1008, 4401, 4403) and reports a SocketRejectedException.
  shouldReconnect: EasySocket.defaultShouldReconnect,
  maxQueuedMessages: 100,       // a dropped message is reported to onError
  connectTimeout: Duration(seconds: 10),
  pingInterval: Duration(seconds: 25),  // null turns the heartbeat off
  pingMessage: null,            // set for browsers, e.g. {'event': 'ping'}
  queryParameters: () async => {...},  // fetched before every connection
  headers: () async => {...},          // not in browsers
  onConnected: (socket) async {...},   // runs before queued messages
  formatDetectionWindow: Duration(milliseconds: 300),
  debug: false,
  decoder: (raw) => SocketEvent(...),  // custom message format
  encoder: (event, data) => ...,
);
```
