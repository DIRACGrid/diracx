## Dependency injection

Dependencies to routes on services are injected as function parameters.
See the FastAPI documentation for details.

In DiracX we have a few additional dependencies which can be injected like so:

```python
@router.get("/dummy")
def my_route(
    config: Annotated[Config, Depends(ConfigSource.create)],
    settings: Annotated[AuthSettings, Depends(AuthSettings.create)],
    job_db: Annotated[JobDB, Depends(JobDB.transaction)],
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
) -> MyReturnType: ...
```
