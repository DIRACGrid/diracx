# How to run the demo

The demo is started by running the `run_demo.sh` script in the `diracx-charts` repository.

```bash
$ cd diracx-charts/
$ ./run_demo.sh ../diracx # (1)!
```

1. Other repositories can also be included here (DIRAC, diracx-web, extensions, ...) which will result in the code be injected into the local instance.

Once the demo has started (~5 minutes) it will print information about how to interact with it:

```md
--8<-- "docs/dev/explanations/run_demo_output.txt"
```

The code changes will be reflected in the demo, in most cases the code will be ["hot-reloaded"](https://en.wikipedia.org/wiki/Hot_swapping#Software_development) when changes are made to the local clone's of the DiracX source code.

## Using the demo

### Running the integration tests

### Using the CLI

### Using the web app

### Using swagger to interact with the API

To login, click the **authorize** button

![authorize](https://diracx-docs-static.s3.cern.ch/assets/images/dev/tutorials/run_locally/login_demo_1.png)

Connect using the authorization code flow, ticking the "vo:diracAdmin" scope

![codeflow](https://diracx-docs-static.s3.cern.ch/assets/images/dev/tutorials/run_locally/login_demo_2.png)

And enter the credentials prompted by the `run_demo.sh` script in the `Dex` interface

![Dexlogin](https://diracx-docs-static.s3.cern.ch/assets/images/dev/tutorials/run_locally/login_demo_3.png)
