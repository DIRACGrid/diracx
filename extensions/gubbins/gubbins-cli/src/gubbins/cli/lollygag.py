"""
This shows how to create a new subcommand
"""

from diracx.cli.utils import AsyncTyper

from gubbins.client.aio import AsyncGubbinsClient

app = AsyncTyper()


@app.command()
def hello():
    """
    This is just to make sure that the CLI extension mechanism works
    """
    print("Shagadelic, Baby!")


@app.async_command()
async def get_owners():
    """
    This makes a proper use of the AsyncGubbinsClient to call
    a method specific to Gubbins
    """
    async with AsyncGubbinsClient() as api:
        owners = await api.lollygag.get_owner_object()
        print(owners)


@app.async_command()
async def sensei():
    """
    This function is only here to test the GUBBINS_SENSEI property
    """
    async with AsyncGubbinsClient() as api:
        secrets = await api.lollygag.get_gubbins_secrets()
        print(secrets)
