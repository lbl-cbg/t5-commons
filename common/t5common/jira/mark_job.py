import typer
from .database import DBConnector, JobState
from .utils import load_info


def mark_job(state: str, directory: str):
    """Mark a job as finished or published.

    This command will be invoked by wokflow and publishing scripts
    to indicate that they are finished.
    """
    issue, database = load_info(directory)
    dbc = DBConnector(f"sqlite:///{database}")
    ret = dbc.transition_job(issue, state)
    dbc.close()
    return ret


def mark_job_main(
    state: JobState = typer.Argument(..., help="The job state to mark"),
    directory: str = typer.Argument("./", help="The directory the job was run from")
):
    """Mark status on a job."""

    ret = mark_job(state.value, directory)
    if not ret:
        exit(1)
