import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:web_socket_channel/status.dart' as status;
import 'package:web_socket_channel/web_socket_channel.dart';

import 'codec.dart';
import 'connect_web.dart' if (dart.library.io) 'connect_io.dart';
import 'socket_event.dart';

/// Handles the payload of a named event.
typedef EventHandler = void Function(Object? data);

/// Creates the underlying channel. Override for custom transports or tests.
///
/// [uri] already includes any `queryParameters`, and [headers] holds the
/// result of `headers` (empty when not set).
typedef ChannelFactory =
    WebSocketChannel Function(
      Uri uri,
      Iterable<String>? protocols,
      Map<String, String> headers,
    );

/// A websocket client that connects lazily, reconnects automatically and
/// routes incoming messages to listeners by event name.
///
/// ```dart
/// final socket = EasySocket('wss://api.example.com/ws');
/// socket.on('message.created', (data) => print(data));
/// socket.emit('chat.send', {'text': 'hello'});
/// ```
class EasySocket {
  /// Creates a socket for [url]. Nothing happens until the first [on],
  /// [stream], [emit], [send] or [connect] call.
  EasySocket(
    String url, {
    this.protocols,
    this.autoReconnect = true,
    this.minReconnectDelay = const Duration(milliseconds: 500),
    this.maxReconnectDelay = const Duration(seconds: 30),
    this.maxReconnectAttempts,
    this.maxQueuedMessages = 100,
    this.formatDetectionWindow = const Duration(milliseconds: 300),
    this.shouldReconnect = defaultShouldReconnect,
    this.connectTimeout = const Duration(seconds: 10),
    this.pingInterval = const Duration(seconds: 25),
    this.pingMessage,
    this.queryParameters,
    this.headers,
    this.onConnected,
    this.debug = false,
    void Function(String message)? logger,
    EventDecoder? decoder,
    EventEncoder? encoder,
    ChannelFactory? channelFactory,
  }) : uri = Uri.parse(url),
       _logger = logger,
       _decoder = decoder,
       _encoder = encoder,
       _channelFactory = channelFactory;

  /// The server address.
  final Uri uri;

  /// Optional websocket sub-protocols.
  final Iterable<String>? protocols;

  /// Whether to reconnect when the connection drops. Defaults to `true`.
  final bool autoReconnect;

  /// Delay before the first retry. Doubles on each failure.
  final Duration minReconnectDelay;

  /// Upper bound for the retry delay.
  final Duration maxReconnectDelay;

  /// Give up after this many consecutive failed attempts. `null` retries
  /// forever. A connection the server closes within a few seconds of opening
  /// counts as a failed attempt.
  final int? maxReconnectAttempts;

  /// Decides whether to reconnect after the server closes the connection
  /// with [closeCode]. When it returns `false`, the socket closes and reports
  /// a [SocketRejectedException] to [onError].
  ///
  /// Defaults to [defaultShouldReconnect].
  final bool Function(int? closeCode, String? closeReason) shouldReconnect;

  /// Reconnects unless the close code says the server refuses this client:
  /// protocol error (1002), unsupported data (1003), invalid data (1007),
  /// policy violation (1008), or the common auth codes 4401 and 4403.
  static bool defaultShouldReconnect(int? closeCode, String? closeReason) =>
      !const {1002, 1003, 1007, 1008, 4401, 4403}.contains(closeCode);

  /// How long opening the connection, including [onConnected], may take
  /// before the attempt counts as failed.
  final Duration connectTimeout;

  /// How often to check that the connection is still alive. `null` turns
  /// the heartbeat off.
  ///
  /// Without a [pingMessage], this uses websocket protocol pings, which every
  /// standard server answers automatically. Browsers can't send those, so on
  /// the web set a [pingMessage] your server replies to.
  final Duration? pingInterval;

  /// A frame to send every [pingInterval] (a `String`, bytes, or anything
  /// JSON-encodable), for servers or platforms without protocol pings. If no
  /// message at all arrives before the next ping is due, the connection is
  /// treated as dead and reopened.
  final Object? pingMessage;

  /// Extra query parameters for the URL, fetched again before every
  /// connection attempt. Use it for auth tokens, so a refreshed token is used
  /// on reconnect:
  ///
  /// ```dart
  /// queryParameters: () async => {'token': await auth.freshToken()},
  /// ```
  final FutureOr<Map<String, String>> Function()? queryParameters;

  /// Headers for the connection request, fetched again before every
  /// attempt. Not supported in browsers; use [queryParameters] there.
  final FutureOr<Map<String, String>> Function()? headers;

  /// Runs after each connection opens, before queued messages are sent and
  /// before the socket counts as connected. Use it to authenticate or
  /// subscribe; messages emitted while it runs go out first.
  ///
  /// It may wait for a server reply (for example
  /// `await socket.stream('ready').first`), within [connectTimeout]. If it
  /// throws, the error goes to [onError] and the socket reconnects.
  final FutureOr<void> Function(EasySocket socket)? onConnected;

  /// How long a connection has to stay open before it counts as healthy and
  /// the retry delay resets.
  static const _stableAfter = Duration(seconds: 5);

  /// Messages sent while disconnected are queued and delivered on connect.
  /// Oldest messages are dropped beyond this limit.
  final int maxQueuedMessages;

  /// After connecting, how long early `emit` calls wait for the server's first
  /// message so they can be sent in the server's format. They go out as soon
  /// as a named event arrives, or with the default `{"event", "data"}` format
  /// when the window passes. Only applies until the format is known, and is
  /// skipped when a custom [decoder] or [encoder] is given.
  final Duration formatDetectionWindow;

  /// Logs connection changes and traffic. Auth values from [queryParameters],
  /// [headers] and credential-like URL parameters are shown as `***`, but
  /// message contents are logged as-is, so keep this off in release builds.
  final bool debug;

  final void Function(String message)? _logger;
  final EventDecoder? _decoder;
  final EventEncoder? _encoder;
  final ChannelFactory? _channelFactory;
  final AutoCodec _codec = AutoCodec();
  final Random _random = Random();

  final Map<String, List<_Listener>> _handlers = {};
  final List<void Function(SocketEvent event)> _anyHandlers = [];
  final List<void Function(Object error)> _errorHandlers = [];
  final List<Object> _queue = [];
  final List<Object> _handshakeQueue = [];
  bool _handshaking = false;
  Timer? _pingTimer;
  bool _awaitingPong = false;
  Timer? _formatTimer;
  bool _formatWindowElapsed = false;
  final StreamController<SocketState> _stateController =
      StreamController.broadcast(sync: true);

  final List<SocketState> _pendingStates = [];
  bool _emittingState = false;
  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _subscription;
  Timer? _reconnectTimer;
  Timer? _stableTimer;
  Timer? _connectTimer;
  Completer<void>? _connected;
  int _attempt = 0;
  int _generation = 0;
  bool _manuallyClosed = false;
  bool _disposed = false;
  SocketState _state = SocketState.idle;

  // ---------------------------------------------------------------- state

  /// The current connection state.
  SocketState get state => _state;

  /// Whether the socket is connected right now.
  bool get isConnected => _state == SocketState.connected;

  /// The close code of the last disconnect, or `null` before the first one.
  int? get closeCode => _closeCode;
  int? _closeCode;

  /// The close reason of the last disconnect, if the server sent one.
  String? get closeReason => _closeReason;
  String? _closeReason;

  /// Emits every state change.
  Stream<SocketState> get onStateChange => _stateController.stream;

  // ------------------------------------------------------------ listening

  /// Calls [handler] with the payload of every [event]. Connects if needed.
  ///
  /// Returns a function that removes the listener.
  void Function() on(String event, EventHandler handler) =>
      _listen(event, _Listener(handler));

  /// Like [on], but removes itself after the first event.
  void Function() once(String event, EventHandler handler) =>
      _listen(event, _Listener(handler, once: true));

  void Function() _listen(String event, _Listener listener) {
    _assertNotDisposed();
    (_handlers[event] ??= []).add(listener);
    _ensureConnected();
    return () => _remove(event, listener);
  }

  void _remove(String event, _Listener listener) {
    final listeners = _handlers[event];
    if (listeners == null) return;
    listeners.remove(listener);
    if (listeners.isEmpty) _handlers.remove(event);
  }

  /// Removes [handler] from [event], or every handler for [event] when
  /// [handler] is omitted. Works for handlers added with [on] or [once].
  void off(String event, [EventHandler? handler]) {
    if (handler == null) {
      _handlers.remove(event);
      return;
    }
    final listeners = _handlers[event];
    final index = listeners?.indexWhere((l) => l.handler == handler) ?? -1;
    if (index >= 0) _remove(event, listeners![index]);
  }

  /// The payloads of [event] as a stream. Connects when listened to.
  Stream<Object?> stream(String event) {
    late final StreamController<Object?> controller;
    void Function()? cancel;
    controller = StreamController<Object?>(
      onListen: () => cancel = on(event, controller.add),
      onCancel: () => cancel?.call(),
    );
    return controller.stream;
  }

  /// Calls [handler] with every decoded event, whatever its name.
  void Function() onAny(void Function(SocketEvent event) handler) {
    _assertNotDisposed();
    _anyHandlers.add(handler);
    _ensureConnected();
    return () => _anyHandlers.remove(handler);
  }

  /// Calls [handler] with connection errors and exceptions thrown by
  /// listeners.
  void Function() onError(void Function(Object error) handler) {
    _errorHandlers.add(handler);
    return () => _errorHandlers.remove(handler);
  }

  // -------------------------------------------------------------- sending

  /// Sends a named event, matching the format the server uses.
  ///
  /// Queued while disconnected and delivered once connected.
  void emit(String event, [Object? data]) {
    _assertNotDisposed();
    if (_handshaking) {
      _handshakeSend(_PendingEvent(event, data));
    } else if (_canWrite && _formatReady) {
      _write(_encode(event, data));
    } else {
      _enqueue(_PendingEvent(event, data));
    }
  }

  /// Sends a raw frame (`String` or `List<int>`) as-is.
  void send(Object frame) {
    _assertNotDisposed();
    if (_handshaking) {
      _handshakeSend(frame);
    } else if (_canWrite) {
      _write(frame);
    } else {
      _enqueue(frame);
    }
  }

  /// Messages sent from [onConnected] skip the main queue.
  void _handshakeSend(Object item) {
    if (_formatReady && _handshakeQueue.isEmpty) {
      _write(item is _PendingEvent ? _encode(item.event, item.data) : item);
    } else {
      _handshakeQueue.add(item);
    }
  }

  bool get _canWrite =>
      _channel != null && _state == SocketState.connected && _queue.isEmpty;

  bool get _formatReady =>
      _encoder != null ||
      _decoder != null ||
      _codec.hasDetectedFormat ||
      _formatWindowElapsed ||
      formatDetectionWindow == Duration.zero;

  Object _encode(String event, Object? data) =>
      _encoder?.call(event, data) ?? _codec.encode(event, data);

  void _write(Object frame) {
    _log('→ $frame');
    try {
      _channel!.sink.add(frame);
    } on StateError {
      // The connection closed but onDone hasn't fired yet. Keep the frame
      // for the next connection.
      _enqueue(frame);
    }
  }

  void _enqueue(Object item) {
    _queue.add(item);
    if (_queue.length > maxQueuedMessages) {
      final dropped = _queue.removeAt(0);
      _reportError(
        dropped is _PendingEvent
            ? MessageDroppedException(dropped.event, dropped.data)
            : MessageDroppedException(null, dropped),
      );
    }
    _ensureConnected();
  }

  // ----------------------------------------------------------- lifecycle

  /// Connects now. Completes once connected; never needed before [on] or
  /// [emit], which connect on their own.
  ///
  /// Throws a [StateError] if the socket is closed before connecting.
  ///
  /// While waiting to retry after a drop, this retries right away instead.
  /// Call it when the network comes back or the app returns to the
  /// foreground.
  Future<void> connect() {
    _assertNotDisposed();
    if (_state == SocketState.connected) return Future.value();
    _manuallyClosed = false;
    final completer = _connected ??= Completer<void>();
    if (_state == SocketState.idle || _state == SocketState.closed) {
      _attempt = 0;
      _open();
    } else if (_reconnectTimer != null) {
      _reconnectTimer!.cancel();
      _reconnectTimer = null;
      _open();
    }
    return completer.future;
  }

  /// Closes the connection and stops reconnecting. Listeners and queued
  /// messages are kept, so a later [connect], [on], [once], [onAny], [emit]
  /// or [send] reopens it.
  Future<void> close() async {
    _manuallyClosed = true;
    _generation++;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _connectTimer?.cancel();
    _connectTimer = null;
    final channel = _channel;
    _detach();
    _setState(SocketState.closed);
    _failPendingConnect(StateError('Socket closed before connecting.'));
    await channel?.sink.close(status.normalClosure);
  }

  /// Closes the socket and releases every listener. The instance can't be
  /// used afterwards.
  Future<void> dispose() async {
    if (_disposed) return;
    await close();
    _disposed = true;
    _handlers.clear();
    _anyHandlers.clear();
    _errorHandlers.clear();
    _queue.clear();
    await _stateController.close();
  }

  // ------------------------------------------------------------ internals

  void _ensureConnected() {
    if (_disposed) return;
    if (_state == SocketState.idle || _state == SocketState.closed) {
      connect().ignore();
    }
  }

  Future<void> _open() async {
    _setState(
      _attempt == 0 ? SocketState.connecting : SocketState.reconnecting,
    );
    final generation = _generation;
    bool stale() => _manuallyClosed || _disposed || generation != _generation;

    WebSocketChannel? channel;
    try {
      final query = await queryParameters?.call();
      final extraHeaders = await headers?.call() ?? const <String, String>{};
      final target = query == null || query.isEmpty
          ? uri
          : uri.replace(queryParameters: {...uri.queryParameters, ...query});
      _rememberSecrets(target, query, extraHeaders);
      _log('connecting to $target (attempt ${_attempt + 1})');
      if (stale()) return;
      channel = _channelFactory != null
          ? _channelFactory(target, protocols, extraHeaders)
          : connectChannel(
              target,
              protocols,
              extraHeaders,
              pingMessage == null ? pingInterval : null,
            );
      await _timed(channel.ready);
    } catch (error) {
      channel?.sink.close().ignore();
      if (stale()) return;
      _reportError(error);
      if (error is UnsupportedError) {
        // Retrying can't help (for example headers in a browser).
        _setState(SocketState.closed);
        _failPendingConnect(error);
        return;
      }
      _scheduleReconnect();
      return;
    }
    if (stale()) {
      channel.sink.close(status.normalClosure).ignore();
      return;
    }

    _channel = channel;
    _awaitingPong = false;
    _subscription = channel.stream.listen(
      _onFrame,
      onError: _reportError,
      onDone: _onDone,
    );
    if (!_formatReady) {
      _formatTimer = Timer(formatDetectionWindow, () {
        _formatWindowElapsed = true;
        _flushQueue();
      });
    }

    final hook = onConnected;
    if (hook != null) {
      _handshaking = true;
      try {
        await _timed(Future.sync(() => hook(this)));
      } catch (error) {
        // A newer connection may own the handshake by now; leave it alone.
        if (stale() || _channel != channel) return;
        _handshaking = false;
        _reportError(error);
        _dropConnection();
        return;
      }
      // The connection may have dropped while the hook ran.
      if (stale() || _channel != channel) return;
      _handshaking = false;
    }

    // Reset the retry delay only once the connection proves healthy, so a
    // server that accepts and then drops us right away isn't hammered.
    _stableTimer = Timer(_stableAfter, () => _attempt = 0);
    _startHeartbeat();
    // Take the completer first: a state listener may close the socket.
    final completer = _connected;
    _connected = null;
    _setState(SocketState.connected);
    completer?.complete();
    if (_formatReady) _flushQueue();
  }

  /// Like `Future.timeout`, but [close] cancels the timer.
  Future<T> _timed<T>(Future<T> future) {
    final result = Completer<T>();
    final timer = Timer(connectTimeout, () {
      if (!result.isCompleted) {
        result.completeError(
          TimeoutException('Connecting took too long.', connectTimeout),
        );
      }
    });
    _connectTimer = timer;
    future.then(
      (value) {
        timer.cancel();
        if (!result.isCompleted) result.complete(value);
      },
      onError: (Object error, StackTrace stack) {
        timer.cancel();
        if (!result.isCompleted) result.completeError(error, stack);
      },
    );
    return result.future;
  }

  void _startHeartbeat() {
    final interval = pingInterval;
    final message = pingMessage;
    if (interval == null || message == null) return;
    final frame = message is String || message is List<int>
        ? message
        : jsonEncode(message);
    _pingTimer = Timer.periodic(interval, (_) {
      if (_awaitingPong) {
        _reportError(
          TimeoutException(
            'No message from the server within $interval of a ping.',
            interval,
          ),
        );
        _dropConnection();
        return;
      }
      _awaitingPong = true;
      _write(frame);
    });
  }

  /// Abandons the current connection and reconnects.
  void _dropConnection() {
    final channel = _channel;
    _detach();
    // Clients may only send 1000 or 3000-4999.
    channel?.sink.close(status.normalClosure).ignore();
    if (_manuallyClosed || _disposed) return;
    _scheduleReconnect();
  }

  void _onFrame(dynamic raw) {
    _log('← $raw');
    _awaitingPong = false;
    final SocketEvent? event;
    try {
      event = _decoder != null ? _decoder(raw) : _codec.decode(raw);
    } catch (error) {
      _reportError(error);
      return;
    }
    if (_formatTimer != null && _codec.hasDetectedFormat) _flushQueue();
    if (event == null) return;
    final SocketEvent decoded = event;

    for (final handler in List.of(_anyHandlers)) {
      _guard(() => handler(decoded));
    }
    final listeners = _handlers[decoded.name];
    if (listeners == null) return;
    for (final listener in List.of(listeners)) {
      if (listener.once) _remove(decoded.name, listener);
      _guard(() => listener.handler(decoded.data));
    }
  }

  void _onDone() {
    final code = _closeCode = _channel?.closeCode;
    final reason = _closeReason = _channel?.closeReason;
    _log('disconnected${code != null ? ' (code $code)' : ''}');
    _detach();
    if (_manuallyClosed || _disposed) return;
    if (!shouldReconnect(code, reason)) {
      _setState(SocketState.closed);
      _reportError(SocketRejectedException(code, reason));
      return;
    }
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    final max = maxReconnectAttempts;
    if (!autoReconnect || (max != null && _attempt >= max)) {
      _setState(SocketState.closed);
      _failPendingConnect(
        StateError(
          'Could not connect to ${uri.scheme}://${uri.authority}${uri.path}.',
        ),
      );
      return;
    }
    final delay = _backoff(_attempt++);
    // Start the timer before announcing the state, so a listener that calls
    // connect() can cut the wait short.
    _reconnectTimer = Timer(delay, () {
      _reconnectTimer = null;
      if (!_manuallyClosed && !_disposed) _open();
    });
    _log('reconnecting in ${delay.inMilliseconds}ms');
    _setState(SocketState.reconnecting);
  }

  Duration _backoff(int attempt) {
    final base = minReconnectDelay.inMilliseconds * pow(2, min(attempt, 20));
    final capped = min(base.toInt(), maxReconnectDelay.inMilliseconds);
    // Jitter between 50% and 100% so many clients don't retry in lockstep.
    return Duration(
      milliseconds: (capped * (0.5 + _random.nextDouble() / 2)).round(),
    );
  }

  void _flushQueue() {
    _formatTimer?.cancel();
    _formatTimer = null;
    if (_channel == null) return;
    final handshake = List.of(_handshakeQueue);
    _handshakeQueue.clear();
    for (final item in handshake) {
      _write(item is _PendingEvent ? _encode(item.event, item.data) : item);
    }
    if (_state != SocketState.connected) return;
    final pending = List.of(_queue);
    _queue.clear();
    for (final item in pending) {
      _write(item is _PendingEvent ? _encode(item.event, item.data) : item);
    }
  }

  void _detach() {
    _formatTimer?.cancel();
    _formatTimer = null;
    _stableTimer?.cancel();
    _stableTimer = null;
    _pingTimer?.cancel();
    _pingTimer = null;
    _handshaking = false;
    _handshakeQueue.clear();
    _subscription?.cancel().ignore();
    _subscription = null;
    _channel = null;
  }

  void _failPendingConnect(Object error) {
    final completer = _connected;
    _connected = null;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(error);
    }
  }

  void _guard(void Function() body) {
    try {
      body();
    } catch (error) {
      _reportError(error);
    }
  }

  void _reportError(Object error) {
    _log('error: $error');
    for (final handler in List.of(_errorHandlers)) {
      try {
        handler(error);
      } catch (_) {
        // Never let an error handler break the socket.
      }
    }
  }

  void _setState(SocketState next) {
    if (_state == next) return;
    _state = next;
    _log('state: ${next.name}');
    // A listener may change the state again while it's being notified; the
    // sync controller can't nest events, so deliver those in order after.
    _pendingStates.add(next);
    if (_emittingState) return;
    _emittingState = true;
    try {
      while (_pendingStates.isNotEmpty) {
        final state = _pendingStates.removeAt(0);
        if (!_stateController.isClosed) _stateController.add(state);
      }
    } finally {
      _emittingState = false;
    }
  }

  /// Values never to print: everything from [queryParameters] and [headers],
  /// and URL query values whose names look like credentials.
  final Set<String> _secrets = {};
  static final _secretName = RegExp(
    'token|key|auth|secret|pass|session|sig|jwt|code',
    caseSensitive: false,
  );

  void _rememberSecrets(
    Uri target,
    Map<String, String>? query,
    Map<String, String> headers,
  ) {
    _secrets
      ..clear()
      ..addAll(query?.values ?? const [])
      ..addAll(headers.values)
      ..addAll([
        for (final MapEntry(:key, :value) in target.queryParameters.entries)
          if (_secretName.hasMatch(key)) value,
      ]);
    _secrets.removeWhere((v) => v.length < 4);
    // Errors and URLs may show them percent-encoded too.
    _secrets.addAll([
      for (final v in List.of(_secrets)) Uri.encodeQueryComponent(v),
    ]);
  }

  void _log(String message) {
    if (!debug) return;
    for (final secret in _secrets) {
      message = message.replaceAll(secret, '***');
    }
    final logger = _logger;
    if (logger != null) {
      logger('[easy_socket] $message');
    } else {
      // ignore: avoid_print
      print('[easy_socket] $message');
    }
  }

  void _assertNotDisposed() {
    if (_disposed) throw StateError('EasySocket used after dispose().');
  }
}

/// An `emit` waiting to be encoded once the server's format is known.
class _PendingEvent {
  const _PendingEvent(this.event, this.data);
  final String event;
  final Object? data;
}

class _Listener {
  _Listener(this.handler, {this.once = false});
  final EventHandler handler;
  final bool once;
}
