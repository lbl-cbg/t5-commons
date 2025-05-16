import typer
from .start_workflow import start_workflows
from .check_workflow import check_workflows
from .publish_results import publish_results
from .mark_job import mark_job_main
from .database import app as database_app

app = typer.Typer(help="Run workflows based on Jira queues and publish results to JAMO", no_args_is_help=True)

app.add_typer(database_app, name="db")
app.command(name="start", no_args_is_help=True)(start_workflows)
app.command(name="check", no_args_is_help=True)(check_workflows)
app.command(name="mark-job", no_args_is_help=True)(mark_job_main)
app.command(name="publish", no_args_is_help=True)(publish_results)


if __name__ == "__main__":
    app()
