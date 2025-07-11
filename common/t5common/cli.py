import typer
from .jira import cli as jira_cli

app = typer.Typer(help="Command-line utilities for the Taskforce5", no_args_is_help=True)
app.add_typer(jira_cli.app, name="jwf")

if __name__ == "__main__":
    app()
