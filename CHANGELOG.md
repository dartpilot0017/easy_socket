## 0.1.1

- `reconnect()` drops the current connection and connects again right away.
  Use it when the app returns to the foreground or the network changes.
- A refused connection is reported to `onError` before the state changes to
  `closed`, so state listeners can already see why.
- Debug logs redact auth values only as whole values, so a short token no
  longer blanks out parts of the URL.
- Link to the live demo.

## 0.1.0

- Initial release: lazy connect, auto-reconnect with backoff, message format
  auto-detection, event routing (`on`, `once`, `off`, `stream`, `onAny`),
  offline send queue, connection state and debug logging.
- Stops reconnecting when the server closes with a "refused" code, and
  reports a `SocketRejectedException`. Override with `shouldReconnect`.
- `onConnected` hook that runs on every connection before queued messages.
- `queryParameters` and `headers`, fetched again before every connection, for
  auth tokens.
- Heartbeat (`pingInterval`, `pingMessage`) that reconnects dead connections.
- `connectTimeout` (10 seconds) for opening the connection and `onConnected`.
- Reports `MessageDroppedException` when the offline queue overflows.
- Events emitted without data no longer send `"data": null`.
- Tested on the Dart VM and in Chrome.
- `connect()` while waiting to retry reconnects right away.
- `closeCode` and `closeReason` of the last disconnect.
- Debug logs show auth values as `***`.
- `example/probe.dart` checks a real server's idle timeout.
