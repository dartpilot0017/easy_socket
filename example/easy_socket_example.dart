import 'package:easy_socket/easy_socket.dart';

void main() {
  final socket = EasySocket('wss://echo.websocket.org', debug: true);

  socket.on('greeting', (data) {
    print('Server said: $data');
    socket.dispose();
  });

  // Sent as {"event":"greeting","data":"hello"}; the echo server sends it back.
  socket.emit('greeting', 'hello');
}
