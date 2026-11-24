# Security policy

## Supported versions

Attesta is pre-1.0. Security fixes go into the latest release only; there are no backports to older tags.

| Version | Supported |
| --- | --- |
| latest `0.x` release | Yes |
| older releases, untagged builds | No |

## Reporting a vulnerability

Do not open a public issue. Email **ngs.gaurav7195@gmail.com** with "Attesta security" in the subject, and include:

- the affected component (`attesta-api`, `attesta-controller`, the CLI, or the generated scaffold) and version or commit,
- how to reproduce it, and what an attacker gains,
- any proof of concept, logs or configuration needed.

Attesta is maintained by one person. You can expect an acknowledgement within 7 days and an assessment within 14. A confirmed issue is fixed in a new release, credited to you unless you ask otherwise, and disclosed in its CHANGELOG entry once the release is out. Please give the fix a chance to ship before publishing details; 90 days is the default if we cannot agree on less.

## In scope

- Authentication or authorization bypass in the API, including role checks per route.
- Anything that lets a request write outside its own records, or read another caller's secrets.
- Injection into generated manifests, workflows or Kubernetes objects.
- The controller changing a Deployment it was not allowed to (without `spec.rollback.enabled` and the Deployment's `attesta.io/rollback-guards` consent).
- Error responses that disclose credentials, hosts or internal detail.
- Supply-chain issues in the release images or the generated CI.

## Out of scope

- Deployments started with `ATTESTA_INSECURE_NO_AUTH=true`, which exists for local development and disables authentication by design.
- Denial of service that requires more traffic than the configured rate limits allow per client.
- Findings in third-party dependencies without a path to exploit them through Attesta; Dependabot and govulncheck track those.
