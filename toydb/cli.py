from urllib.parse import urljoin

import httpx
import typer
from rich.console import Console
from rich.prompt import Confirm


app = typer.Typer(no_args_is_help=True)
state = {}
console = Console()


@app.callback()
def main(url: str = typer.Option(envvar="TOYDB_URL")):
    """Interact with ToyDB!"""
    state["url"] = url


@app.command()
def get(key: str):
    """Get the value behind the key from the DB."""
    with console.status(f"[bold green]Getting key '{key}'..."):
        endpoint = urljoin(state["url"], f"v1/db/{key}")
        try:
            result = httpx.get(endpoint)
        except httpx.ConnectError as error:
            console.print(error)
            return 1
        if result.is_success:
            console.print(result.json())
        elif result.is_client_error:
            console.print(result.json())
            return 1
        else:
            console.print("Unknown server error.")
            return 1
        return 0


@app.command(name="set")
def set_(key: str, value: str) -> int:
    """Set the key to the value."""
    with console.status(f"[bold green]Setting key '{key}'..."):
        endpoint = urljoin(state["url"], f"v1/db/{key}")
        try:
            result = httpx.post(endpoint, json={"value": value})
        except httpx.ConnectError as error:
            console.print(error)
            return 1
        if result.is_success:
            console.print(f"Successfully set key '{key}'.")
        elif result.is_client_error:
            console.print(result.json()["details"])
            return 1
        else:
            console.print("Unknown server error.")
            return 1
        return 0


@app.command()
def delete(key: str) -> int:
    """Delete the given key."""
    with console.status(f"[bold green]Deleting key '{key}'..."):
        endpoint = urljoin(state["url"], f"v1/db/{key}")
        try:
            result = httpx.delete(endpoint)
        except httpx.ConnectError as error:
            console.print(error)
            return 1
        if result.is_success:
            console.print(f"Successfully deleted key '{key}'.")
        elif result.is_client_error:
            console.print(result.json()["details"])
            return 1
        else:
            console.print("Unknown server error.")
            return 1
        return 0


@app.command()
def drop(force: bool = False) -> int:
    """Drops the database."""
    if not force:
        if not Confirm.ask("Are you sure?"):
            return 1
    endpoint = urljoin(state["url"], "v1/db")
    try:
        result = httpx.delete(endpoint)
    except httpx.ConnectError as error:
        console.print(error)
        return 1
    if result.is_success:
        console.print("Successfully dropped database.")
    elif result.is_client_error:
        console.print(result.json()["details"])
        return 1
    else:
        console.print("Unknown server error.")
        return 1
    return 0


if __name__ == "__main__":
    app()
