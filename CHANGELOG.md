# Changelog
## 0.6.0
  * Made new changes to add metadata fields and refactoring code. [#37](https://github.com/singer-io/tap-gitlab/pull/37)
  * Update to check api credentials before running discovery and support comma/space separated projects and groups list in config. [#46](https://github.com/singer-io/tap-gitlab/pull/46)

## 0.5.3
  * Moves private token from the params to the header "PRIVATE-TOKEN" [#44](https://github.com/singer-io/tap-gitlab/pull/44)

## 0.5.2
  * Bump dependency versions for twistlock compliance
  * Update circle config to fix failing build
  * [#40](https://github.com/singer-io/tap-gitlab/pull/40)

## 0.5.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.5.0
  * Added support for groups and group milestones [#9](https://github.com/singer-io/tap-gitlab/pull/9)
