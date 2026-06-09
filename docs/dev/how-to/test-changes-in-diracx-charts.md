# Testing against changes in diracx-charts

Changes in `diracx` may require corresponding changes in `diracx-charts` (typically adding a new database for example).

## Testing locally

[run_demo.sh](../explanations/run_demo.md) allows to specify your local version of `diracx`, or `diracx-web`.

```bash
$ cd diracx-charts/
$ ./run_demo.sh ../diracx
```

See the [tutorial](../tutorials/run-locally.md) for detailed example.

## Running the CI

Since the changes are interdependent, it is expected the `diracx` CI to fail. The good practice here is to add a separate commit to your `diracx` PR to point the CI to your own `diracx-charts` branch instead of the master.

This is done in `.github/workflows/main.yml`:

```yaml
env:
  DIRACX_CHARTS_REPOSITORY: https://github.com/DIRACGrid/diracx-charts.git
  DIRACX_CHARTS_BRANCH: master
```
