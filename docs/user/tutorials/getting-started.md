# Install DiracX client

`DiracX` comes with a few utilities to ease its installation.

<<<<<<< HEAD
```bash
pip install diracx
=======
```bash  title="<code>$ pip install diracx</code>"
Installing collected packages: wrapt, urllib3, typing-extensions, sniffio, smmap, six, shellingham, sh, pyyaml, python-dotenv, pyjwt, pygments, pycparser, propcache, multidict, mdurl, jmespath, isodate, idna, h11, frozenlist, dnspython, diraccfg, click, charset_normalizer, certifi, cachetools, attrs, annotated-types, aioitertools, aiohappyeyeballs, yarl, typing-inspection, requests, python-dateutil, pydantic-core, markdown-it-py, httpcore, gitdb, email-validator, cffi, anyio, aiosignal, rich, pydantic, httpx, gitpython, cryptography, botocore, azure-core, aiohttp, typer, pydantic-settings, joserfc, aiobotocore, diracx-core, diracx-client, diracx-api, diracx-cli, diracx
Successfully installed aiobotocore-2.23.2 aiohappyeyeballs-2.6.1 aiohttp-3.12.15 aioitertools-0.12.0 aiosignal-1.4.0 annotated-types-0.7.0 anyio-4.10.0 attrs-25.3.0 azure-core-1.35.0 botocore-1.39.8 cachetools-6.1.0 certifi-2025.8.3 cffi-1.17.1 charset_normalizer-3.4.2 click-8.2.1 cryptography-45.0.6 diraccfg-1.0.1 diracx-0.0.1a46 diracx-api-0.0.1a46 diracx-cli-0.0.1a46 diracx-client-0.0.1a46 diracx-core-0.0.1a46 dnspython-2.7.0 email-validator-2.2.0 frozenlist-1.7.0 gitdb-4.0.12 gitpython-3.1.45 h11-0.16.0 httpcore-1.0.9 httpx-0.28.1 idna-3.10 isodate-0.7.2 jmespath-1.0.1 joserfc-1.2.2 markdown-it-py-3.0.0 mdurl-0.1.2 multidict-6.6.3 propcache-0.3.2 pycparser-2.22 pydantic-2.11.7 pydantic-core-2.33.2 pydantic-settings-2.10.1 pygments-2.19.2 pyjwt-2.10.1 python-dateutil-2.9.0.post0 python-dotenv-1.1.1 pyyaml-6.0.2 requests-2.32.4 rich-14.1.0 sh-2.2.2 shellingham-1.5.4 six-1.17.0 smmap-5.0.2 sniffio-1.3.1 typer-0.16.0 typing-extensions-4.14.1 typing-inspection-0.4.1 urllib3-2.5.0 wrapt-1.17.2 yarl-1.20.1

>>>>>>> 6794298 (docs: added note before converting CS)
```

Congrats, you have just installed the `diracx` client.

```bash  title="<code>$ dirac --help</code>"

 Usage: dirac [OPTIONS] COMMAND [ARGS]...

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --output-format             TEXT  [default: None]                                                                                                                                                                                          │
│ --install-completion              Install completion for the current shell.                                                                                                                                                                │
│ --show-completion                 Show completion for the current shell, to copy it or customize the installation.                                                                                                                         │
│ --help                            Show this message and exit.                                                                                                                                                                              │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ login    Login to the DIRAC system using the device flow.                                                                                                                                                                                  │
│ whoami                                                                                                                                                                                                                                     │
│ logout                                                                                                                                                                                                                                     │
│ config                                                                                                                                                                                                                                     │
│ jobs                                                                                                                                                                                                                                       │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

```
