# Security Policy

## Reporting a vulnerability

Please report suspected security vulnerabilities privately rather than posting
an unpatched issue or pull request.

**[TODO: Add a verified private maintainer email or vulnerability-reporting
channel before sharing this policy publicly.]**

Useful report information includes:

- affected DAG, module, configuration, or dependency;
- steps that reproduce the issue;
- expected and actual behavior;
- relevant logs with secrets and personal data removed;
- a suggested mitigation, if available.

Do not include credentials, API keys, or private Open-Meteo or LLM connection
details in a report.

## Scope

The repository and the Airflow workflows, data processing code, Pydantic AI
tools, and report-generation paths are in scope.

The project does not define a public list of supported versions or maintained
release branches. Until maintainers provide one, report findings against the
latest repository revision.

## Repository-facing concerns

If you find secrets, credentials, or generated local configuration data in the
repository, do not publish their contents. Contact the maintainers through the
private channel above and include only the minimum information needed to locate
and remove the exposure.
