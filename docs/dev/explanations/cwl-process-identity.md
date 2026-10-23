# CWL process identity

Every CWL process DiracX stores is keyed by a hash of its content
(DX-ADR-004, DX-ADR-007), computed by `diracx.core.cwl` from the document the
client submits.

## From files to identities

The client packs its files with `cwltool --pack` into one document: a
`$graph` of processes, each named after its file, where a step's `run` names
another entry. For the MCSimulation workgraph (issue #1080) that is six entries:

```mermaid
flowchart TD
    main["#main (the workgraph)"] --> gauss["#gauss.cwl"]
    main --> reco["#boole-moore.cwl"]
    main --> merge["#merge.cwl"]
    reco --> digi["#digi-reco.cwl"]
    gauss --> tool["#lb-run-app.cwl"]
    digi --> tool
    merge --> tool
```

The services first check that the document is self-contained
(`check_self_contained`), so that nothing ever fetches anything on its behalf:

- no `$` key but the document's own `$graph` and `$namespaces`;
- every `run` is an entry of the `$graph` or an inline body;
- `File` and `Directory` literals carry their `contents` or `listing`
    inline, never a `path` or `location`.

Then `decompose` hashes each process with `diracx.core.hashing.hash_json`, as
submitted, except that each step's `run` is replaced by the identity of the
process it runs (`proc:sha256-<hex>`). Children are hashed before their
parents, so a parent's identity covers everything below it (a Merkle tree):

| Entry              | Identity        | Runs                      |
| ------------------ | --------------- | ------------------------- |
| `#lb-run-app.cwl`  | `80d85694a863…` | nothing                   |
| `#gauss.cwl`       | `e1e266130b5d…` | `80d85694a863…`           |
| `#digi-reco.cwl`   | `89aa5fb8d5f8…` | `80d85694a863…`           |
| `#boole-moore.cwl` | `31cd3f1f36f1…` | `89aa5fb8d5f8…`           |
| `#merge.cwl`       | `9d84d8c7268c…` | `80d85694a863…`           |
| `#main`            | `db52479ec2d7…` | the three transformations |

An edit to `lb-run-app.cwl` changes every identity in the table; an edit to
`merge.cwl` changes only its own and the workgraph's. An inline `run` body is
hashed the same way, so the same process inline or by `#id` has one identity.

`subgraph(document, "#merge.cwl")` returns the entry and everything it runs,
as submitted: the document a stock runner executes for one transformation,
and which re-hashes to the same identities.

## What is not normalised

The identity is the hash of what was submitted. The same files packed by
another cwltool version, with an edited `doc`, or under another file name get
other identities. That only costs a duplicate row, and it means DiracX never
rewrites a document: what is stored is what cwltool resolved, which is what
the user tested.
