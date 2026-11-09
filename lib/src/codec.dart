import 'dart:convert';

import 'socket_event.dart';

/// Turns a raw websocket frame into a [SocketEvent]. Return `null` to ignore
/// the frame.
typedef EventDecoder = SocketEvent? Function(Object? raw);

/// Turns an event name and payload into a frame to send.
typedef EventEncoder = Object Function(String event, Object? data);

/// Detects the server's message format from incoming frames and mirrors it
/// when sending.
///
/// Recognised shapes:
/// * `{"event"|"type"|"action": "name", "data"|"payload"|"body": ...}`
/// * flat objects: `{"type": "name", "text": "hi"}` (payload is the rest of
///   the object)
/// * arrays: `["name", data]`
/// * anything else (plain text, binary, other JSON) becomes a `message` event.
///
/// Until a named event has been received, outgoing events use
/// `{"event": name, "data": data}`.
class AutoCodec {
  /// Keys checked, in order, for the event name.
  static const nameKeys = ['event', 'type', 'action'];

  /// Keys checked, in order, for the payload.
  static const dataKeys = ['data', 'payload', 'body'];

  _Format _format = const _ObjectFormat('event', 'data');
  bool _learned = false;

  /// Whether a format has been detected from an incoming message.
  bool get hasDetectedFormat => _learned;

  /// Decodes [raw] and remembers its shape for [encode].
  SocketEvent decode(Object? raw) {
    final Object? value;
    if (raw is String) {
      try {
        value = jsonDecode(raw);
      } on FormatException {
        return SocketEvent(SocketEvent.fallbackName, raw, raw);
      }
    } else {
      return SocketEvent(SocketEvent.fallbackName, raw, raw);
    }

    if (value is Map<String, dynamic>) {
      for (final nameKey in nameKeys) {
        final name = value[nameKey];
        if (name is! String) continue;
        for (final dataKey in dataKeys) {
          if (value.containsKey(dataKey)) {
            _learn(_ObjectFormat(nameKey, dataKey));
            return SocketEvent(name, value[dataKey], raw);
          }
        }
        _learn(_FlatFormat(nameKey));
        return SocketEvent(name, Map.of(value)..remove(nameKey), raw);
      }
    } else if (value is List && value.length == 2 && value.first is String) {
      _learn(const _ArrayFormat());
      return SocketEvent(value.first as String, value.last, raw);
    }
    return SocketEvent(SocketEvent.fallbackName, value, raw);
  }

  /// Encodes an outgoing event in the detected format.
  String encode(String event, Object? data) =>
      jsonEncode(_format.encode(event, data));

  void _learn(_Format format) {
    if (_learned) return;
    _format = format;
    _learned = true;
  }
}

sealed class _Format {
  const _Format();
  Object encode(String event, Object? data);
}

final class _ObjectFormat extends _Format {
  const _ObjectFormat(this.nameKey, this.dataKey);
  final String nameKey;
  final String dataKey;

  @override
  Object encode(String event, Object? data) => {nameKey: event, dataKey: ?data};
}

final class _FlatFormat extends _Format {
  const _FlatFormat(this.nameKey);
  final String nameKey;

  @override
  Object encode(String event, Object? data) =>
      data is Map ? {nameKey: event, ...data} : {nameKey: event, 'data': ?data};
}

final class _ArrayFormat extends _Format {
  const _ArrayFormat();

  @override
  Object encode(String event, Object? data) => [event, data];
}
