## 0.1.2

- A connection request the server answers with HTTP 4xx (401 Unauthorized,
  403 Forbidden, 404…) now counts as refused: the socket stops retrying and
  reports a `SocketRejectedException` with the new `httpStatus` field, and
  `code` set to `4000 + status`. 408 and 429 are still retried. Works on the
  Dart VM (mobile, desktop, server); browsers hide the status, so on the web
  these are still retried.
- `defaultShouldReconnect` now refuses close codes 4400 to 4499 (it used to
  refuse only 4401 and 4403), except 4408 and 4429.
- The refusal is reported without the raw connection error, which contains
  the full URL including any token.

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
