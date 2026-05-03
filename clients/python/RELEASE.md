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

## Release process

The release workflow is `.github/workflows/release-python.yml`.

1. Update `clients/python/pyproject.toml` version and any release notes/changelog if present.
2. Merge the release prep PR.
3. In PyPI, configure Trusted Publisher for:
   - repository: `NikolayS/pgque`
   - workflow: `release-python.yml`
   - environment: `pypi`
   - package: `pgque-py`
4. In TestPyPI, configure the same workflow with environment `testpypi`.
5. Run **Release Python client** with `dry_run=true` first.
6. Run it with `dry_run=false` and `repository=testpypi`.
7. Verify the TestPyPI artifact installs in a clean environment, using PyPI
   as the extra index for dependencies:

   ```bash
   python -m pip install \
     --index-url https://test.pypi.org/simple \
     --extra-index-url https://pypi.org/simple \
     pgque-py
   ```
8. Run the workflow again with `dry_run=false` and `repository=pypi`.

The workflow builds with `python -m build`, validates with `twine check`, and
publishes via PyPI Trusted Publisher / OIDC. No long-lived PyPI token is needed.
