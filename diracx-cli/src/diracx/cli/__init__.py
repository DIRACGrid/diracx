from __future__ import annotations

from diracx.core.extensions import DiracEntryPoint, select_from_extension

from .auth import app

__all__ = ("app",)


# Load all the sub commands
cli_names = set(
    [
        entry_point.name
        for entry_point in select_from_extension(group=DiracEntryPoint.CLI)
    ]
)
for cli_name in cli_names:
    entry_point = select_from_extension(group=DiracEntryPoint.CLI, name=cli_name)[0]
    app.add_typer(entry_point.load(), name=entry_point.name)


cli_hidden_names = set(
    [
        entry_point.name
        for entry_point in select_from_extension(group=DiracEntryPoint.HIDDEN_CLI)
    ]
)
for cli_name in cli_hidden_names:
    entry_point = select_from_extension(
        group=DiracEntryPoint.HIDDEN_CLI, name=cli_name
    )[0]
    app.add_typer(entry_point.load(), name=entry_point.name, hidden=True)


if __name__ == "__main__":
    app()
