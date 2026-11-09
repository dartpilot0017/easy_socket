import 'package:web_socket_channel/web_socket_channel.dart';

/// Opens a channel in the browser, which can't send custom headers or
/// protocol-level pings.
WebSocketChannel connectChannel(
  Uri uri,
  Iterable<String>? protocols,
  Map<String, String> headers,
  Duration? pingInterval,
) {
  if (headers.isNotEmpty) {
    throw UnsupportedError(
      'Browsers do not allow custom websocket headers. '
      'Send the token with queryParameters instead.',
    );
  }
  return WebSocketChannel.connect(uri, protocols: protocols);
}
