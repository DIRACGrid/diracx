
# Gubbins

``gubbins`` is a ``diracx`` extension. It is a show-case for everything which is possible to extend.

It should also serve as a reference doc on how to write your own extension. Everything in the ``diracx`` dev documentation applies here too.

If you write your own extension, just replace ``gubbins`` with whatever name you chose (e.g. ``lhcbdiracx``, ``belle2diracx``, ``donotforgetgriddpp``).

The structure of the repo, the content of the ``pyproject.toml`` files, the ``__init__`` of the modules... are all skeletons that you must reproduce.

Extensions are enabled via the ``DIRACX_EXTENSIONS`` environment variable. It should be set to ``gubbins,diracx``

## QUESTIONS

What to do with the `environment.yaml` ? should we replicate wht's in diracx ?


## CI

What is under ``.github/workflows/extensions.yaml`` should in fact be split in multiple jobs under ``.github/workflows/`` of your repo

Here we make sure to use the local versions of ``diracx`` packages to make sure we are always up to date, and that a change in ``diracx`` does not break the extension mechanisms. But in your real extension, you will want to depend on published package (i.e. ``pip install diracx-routers`` instead of ``pip install -e ./diracx-routers`` for example), and on published docker images

## Installing the extension

To develop, follow the same instruction as ``diracx``

Do not forget to add the database in your `[project.entry-points."diracx.db.sql"]`

## DB

### New DB

``lollygag`` is a DB which is specific to ``gubbins``, i.e. it does not modify or extend an existing ``diracx`` db

### Extended DB

``GubbinsJobDB`` illustrate how to extend an existing ``diracx`` DB, add new methods, modify methods, add a table.



## Routers

In order to add/extend routers, you need to create a router submodule

``lollygag`` is a router which is specific to ``gubbins``, i.e. it does not modify or extend an existing ``diracx`` routers. It uses the ``lollygagDB``

## Writing tests

getting unit tests to run

You MUST have a ``gubbins-testing`` in order to define the session scope that will set the ``DIRACX_EXTENSIONS`` env variable.

we need DIRACX_EXTENSIONS=gubbins,diracx
