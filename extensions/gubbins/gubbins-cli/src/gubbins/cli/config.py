"""
This just shows how to extend and modify an existing CLI
"""

# In order to extend it, just import the app from DiracX
from diracx.cli.config import app


@app.async_command()
async def gubbins_extra():
    """
    Add an extra command
    """
    print("Adding something extra")


@app.async_command()
async def dump():
    """Replace a command"""
    print("I replaced the original dump")
