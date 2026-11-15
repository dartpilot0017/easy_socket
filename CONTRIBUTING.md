# Contributing

Thanks for helping out! A few rules keep `main` releasable.

## Branches

- **`dev`** is where work lands. Open every pull request against `dev`.
- **`main`** holds released code. It only accepts pull requests from `dev`;
  a pull request into `main` from any other branch fails the required
  `from-dev` check and can't be merged.
- Nobody pushes to `main` or `dev` directly. Both need a pull request,
  passing checks and the maintainer's approval.

## Before opening a pull request

```bash
dart format .
dart analyze --fatal-infos
dart test -p vm,chrome
```

Every pull request also runs a malware scan, a secret scan, a dependency
vulnerability check and a check for risky code (native code, running
programs, build hooks, binaries, new dependencies). If that last check flags
your change, explain why in the pull request; the maintainer reviews it and
can approve it with the `security-reviewed` label.

## Reporting security issues

Please don't open a public issue. See [SECURITY.md](SECURITY.md).
