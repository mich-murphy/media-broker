# Security policy

media-broker is an authenticated read-only boundary between an AI agent and
household media services, so vulnerability reports are welcome.

## Reporting

Report suspected vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/mich-murphy/media-broker/security/advisories/new).
Do not open a public issue for anything that could be exploited.

Please include the affected version or image digest, a description of the
impact, and reproduction steps. Reports are acknowledged within seven days.

## Scope

In scope: authentication bypass, Host or Origin allow-list bypass, leakage of
upstream credentials or unprojected upstream data, and any way to make the
broker perform a non-read-only or unbounded upstream request.

Out of scope: the security of the upstream services themselves, and
deployments that disable the documented defaults.

## Supported versions

Only the current `main` image is supported. Fixes ship as a new
`ghcr.io/mich-murphy/media-broker:main` digest.
