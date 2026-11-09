import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Opens a channel with dart:io, which supports headers and protocol-level
/// pings.
WebSocketChannel connectChannel(
  Uri uri,
  Iterable<String>? protocols,
  Map<String, String> headers,
  Duration? pingInterval,
) => IOWebSocketChannel.connect(
  uri,
  protocols: protocols,
  headers: headers.isEmpty ? null : headers,
  pingInterval: pingInterval,
);
