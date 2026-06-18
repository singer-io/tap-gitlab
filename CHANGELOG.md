# Changelog


# 1.2.0
  * Streams returning 403 Forbidden during discovery are now excluded from the catalog; discovery fails only if none are accessible. [#52](https://github.com/singer-io/tap-gitlab/pull/52)
# 1.1.0
  * Add optional `api_url` config field to support on-premises GitLab instances; defaults to `https://gitlab.com`. [#51](https://github.com/singer-io/tap-gitlab/pull/51)
  * Bump singer-python to 6.8.0 and requests to 2.34.2.

# 1.0.1
  * Bump requests to 2.33.0 for security updates [#50](https://github.com/singer-io/tap-gitlab/pull/50)

## 1.0.0
  * Made new changes to add metadata fields and refactoring code. Removed `api_url` from required config field list. [#37](https://github.com/singer-io/tap-gitlab/pull/37)
  * Update to check api credentials before running discovery and support comma/space separated projects and groups list in config. Updated unset USE_STITCH_BACKEND in config.yaml due to Stitch backend need to be updated with current changes(remove api_url field). [#46](https://github.com/singer-io/tap-gitlab/pull/46)

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
