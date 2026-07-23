# Security Policy

## Supported Versions

| Version | Security fixes |
| --- | --- |
| 1.1.x | Yes |
| 1.0.x | Critical compatibility and migration fixes during the 1.1 rollout |
| Older | No |

## Reporting A Vulnerability

Do not open a public issue containing an exploit, credential, tenant data, or
unpatched vulnerability detail. Use the repository host's private security
advisory channel or contact the project maintainers through the private
engineering support channel. Include:

- affected version and build commit from `graphdb version`;
- deployment mode and storage/coordinator type;
- minimal reproduction and impact;
- whether the data/admin listeners were reachable without the gateway;
- proposed disclosure timing, if applicable.

Maintainers should acknowledge a report, assign severity and owner, reproduce
it in an isolated environment, and coordinate a fixed release before public
disclosure.

## Deployment Boundary

GGraphDB does not authenticate callers itself. `X-Tenant-ID` is routing metadata,
not identity. Production deployments must use the controls in
[docs/security-deployment.md](docs/security-deployment.md), including TLS,
authentication, tenant-header replacement, RBAC, listener isolation, and
credential scoping.

Never attach object-store data, PostgreSQL coordination rows, access tokens, or
customer graph exports to a public report.
