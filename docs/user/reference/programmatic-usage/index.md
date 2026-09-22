# Programmatic usage

It is often useful to programmatically interact with DiracX to perform actions that would be infeasible (or just tedious) to do manually.
There are three options available for doing this:

- [**Command Line Interface:**](./command-line-interface.md) Most of the command line interface is designed to be used either interactively or programmatically. This is often most useful for quick one-off interactions where you write a [one-liner](https://en.wikipedia.org/wiki/One-liner_program) or small shell script.
- [**Python API:**](./python-interface.md) DiracX provides a rich Python interface which exposes all functionality. This is most useful when writing Python code which should interact with DiracX for everything from one-off scripts to rich applications which integrate which the resources provided by DiracX.
- [**HTTPS API:**](./https-interface.md) All communication between the above options and DiracX is done over HTTPS therefore you can choose to interact with DiracX using HTTPS directly. This is most useful for integrating with programming languages other than Python, or in environments where it is undesirable to install the full DiracX client.
