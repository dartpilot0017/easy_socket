/// A single decoded message received from (or sent to) the server.
class SocketEvent {
  /// Creates an event with a [name] and optional [data].
  const SocketEvent(this.name, [this.data, this.raw]);

  /// The event name, e.g. `message.created`.
  ///
  /// Messages without a recognisable name use [SocketEvent.fallbackName].
  final String name;

  /// The event payload.
  final Object? data;

  /// The original frame as it came off the wire (a `String` or `List<int>`).
  final Object? raw;

  /// The name given to messages that carry no event name (plain text, binary
  /// frames, or JSON without an `event`/`type`/`action` key).
  static const fallbackName = 'message';

  @override
  String toString() => 'SocketEvent($name, $data)';
}

/// The lifecycle state of an `EasySocket`.
enum SocketState {
  /// Created but not yet connected. Connects on first `on`/`emit`/`connect`.
  idle,

  /// Opening the first connection.
  connecting,

  /// Connected and ready.
  connected,

  /// The connection dropped and a retry is pending or in progress.
  reconnecting,

  /// Closed by `close()`, or retries were exhausted.
  closed,
}

/// Reported to `onError` when the server closes the connection with a code
/// that means retrying won't help (see `EasySocket.shouldReconnect`). The
/// socket stays closed until it is used again.
class SocketRejectedException implements Exception {
  /// Creates the exception for a close [code] and [reason].
  const SocketRejectedException(this.code, this.reason);

  /// The websocket close code the server sent.
  final int? code;

  /// The close reason the server sent, if any.
  final String? reason;

  @override
  String toString() =>
      'SocketRejectedException: the server closed the connection with code '
      '$code${reason != null && reason!.isNotEmpty ? ' ($reason)' : ''}.';
}

/// Reported to `onError` when the offline queue is full and the oldest
/// message is dropped (see `EasySocket.maxQueuedMessages`).
class MessageDroppedException implements Exception {
  /// Creates the exception for a dropped [event] or raw frame.
  const MessageDroppedException(this.event, this.data);

  /// The event name, or `null` for a raw frame sent with `send`.
  final String? event;

  /// The event payload, or the raw frame.
  final Object? data;

  @override
  String toString() =>
      'MessageDroppedException: the offline queue is full, so '
      '${event != null ? 'the "$event" event' : 'a raw frame'} was dropped.';
}
