# Contributing

Thanks for helping out! A few rules keep `main` releasable.

## Branches

- **`dev`** is where work lands. Open every pull request against `dev`.
- **`main`** holds released code. It only accepts pull requests from `dev`;
  a pull request into `main` from any other branch fails the required
  `from-dev` check and can't be merged.
- Nobody pushes to `main` or `dev` directly. Both need a pull request,
  passing checks and the maintainer's approval.

## Releases

1. Bump `version:` in `pubspec.yaml` and add a `CHANGELOG.md` entry in your
   pull request to `dev`.
2. Merge `dev` into `main`.
3. The **Tag release** workflow tags the commit that set the version
   (`v1.2.3`) automatically. There's no need to tag by hand.
4. Publish with `dart pub publish` from `main`.

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
