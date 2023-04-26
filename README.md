# Client  Generation

File written by hand: src/dirac/operations/_patch.py

docker run -it -v "${PWD}:/generated"  --entrypoint=/bin/sh --net host  --rm azsdkengsys.azurecr.io/azuresdk/autorest-python

autorest --python --input-file='http://localhost:8000/openapi.json' --models-mode=msrest

```python
In [4]: import  dirac
   ...: with open('/tmp/dirac_token.json', 'r') as f:
   ...:     token = f.read()
   ...: api = dirac.Dirac(endpoint='http://localhost:8000/', headers = {"Authorization":f"Bearer {token}"})
   ...: api.jobs.set_status_bulk([{"job_id":1, "status":"Running"}])
Out[4]: [<dirac.models._models.JobStatusReturn at 0x7f18c5a1de10>]
```
