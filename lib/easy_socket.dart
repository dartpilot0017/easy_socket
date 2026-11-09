/// Websocket events with no setup: lazy connect, auto-reconnect,
/// message-format detection and event routing.
library;

export 'src/codec.dart' show AutoCodec, EventDecoder, EventEncoder;
export 'src/easy_socket.dart' show ChannelFactory, EasySocket, EventHandler;
export 'src/socket_event.dart';
