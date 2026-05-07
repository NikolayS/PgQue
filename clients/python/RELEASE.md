# Python client release

Distribution name: `pgque-py` on PyPI. Import package: `pgque`.

`pgque` is already taken on PyPI by an unrelated project, so PgQue's Python
client uses a distinct distribution name while preserving the natural import:

```bash
pip install pgque-py
```

```python
import pgque
```

## Versioning

The Python client version is independent from the SQL/server
`pgque.version()`. Bump this package when the Python API or packaging changes;
server-only SQL changes do not require a Python client release.

Use Python/PEP 440 version strings in `pyproject.toml`. For a development or
pre-release build, use forms like `0.2.0.dev0` or `0.2.0rc1`; do **not** use
Git-style `0.2.0-dev`, which PyPI rejects.

## GitHub environment prerequisite

Before the first real publish, create GitHub environments in `NikolayS/pgque`:

- `testpypi`
- `pypi`

Protect them as appropriate for releases (for example, required reviewers and
`main` branch restrictions). The workflow also checks that it is running from
`main`, but environment protection is the human approval gate.

## Release process

The release workflow is `.github/workflows/release-python.yml`.

1. Update `clients/python/pyproject.toml` version and any release notes/changelog if present.
2. Merge the release prep PR.
3. Ensure the `testpypi` and `pypi` GitHub environments exist and are protected.
4. In PyPI, configure Trusted Publisher for:
   - repository: `NikolayS/pgque`
   - workflow: `release-python.yml`
   - environment: `pypi`
   - package: `pgque-py`
5. In TestPyPI, configure the same workflow with environment `testpypi`.
6. Run **Release Python client** with `dry_run=true` first. Dry runs only build
   and validate artifacts; they do not require `testpypi` / `pypi` environment
   approval or OIDC permissions.
7. Run it with `dry_run=false` and `repository=testpypi`.
8. Verify the TestPyPI artifact installs in a clean environment, using PyPI
   as the extra index for dependencies:

   ```bash
   python -m pip install \
     --index-url https://test.pypi.org/simple \
     --extra-index-url https://pypi.org/simple \
     pgque-py
   ```
9. Run the workflow again with `dry_run=false` and `repository=pypi`.

The workflow builds with `python -m build`, validates with `twine check`, and
publishes via PyPI Trusted Publisher / OIDC. No long-lived PyPI token is needed.
