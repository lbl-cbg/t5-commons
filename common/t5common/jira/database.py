import argparse
import sqlite3
from pathlib import Path
from enum import Enum

import typer

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DDL
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import event

from .utils import load_config
from ..utils import get_logger

app = typer.Typer(help="Commands for managing workflow database", no_args_is_help=True)

Base = declarative_base()


class JobState(Enum):
    WORKFLOW_STARTED = 'WORKFLOW_STARTED'
    WORKFLOW_START_FAILED = 'WORKFLOW_START_FAILED'
    WORKFLOW_CHECK_FAILED = 'WORKFLOW_CHECK_FAILED'
    WORKFLOW_FINISHED = 'WORKFLOW_FINISHED'
    PUBLISH_FAILED = 'PUBLISH_FAILED'
    PUBLISHED = 'PUBLISHED'
    WORKFLOW_FAILED = 'WORKFLOW_FAILED'

class JobStates(Base):
    __tablename__ = 'job_states'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    jobs = relationship("Job", back_populates="job_state")

class Job(Base):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True)
    issue = Column(String, nullable=False)
    job_directory = Column(String, nullable=False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    job_state_id = Column(Integer, ForeignKey('job_states.id'), nullable=False)

    project = relationship("Project", back_populates="jobs")
    job_state = relationship("JobStates", back_populates="jobs")


class Project(Base):
    __tablename__ = 'project'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    jobs = relationship("Job", back_populates="project")


class JobStateHistory(Base):
    __tablename__ = 'job_state_history'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('job.id'), nullable=False)
    job_state_id = Column(Integer, ForeignKey('job_states.id'), nullable=False)
    timestamp = Column(String, nullable=False)


def initialize_database(database):
    conn_str = f"sqlite:///{database}"

    # Create an SQLite database and the tables
    engine = create_engine(conn_str)
    Base.metadata.create_all(engine)

    # Create the trigger for tracking job state changes
    trigger = DDL("""
    CREATE TRIGGER track_job_state_change
    AFTER UPDATE OF job_state_id ON job
    FOR EACH ROW
    BEGIN
        INSERT INTO job_state_history (job_id, job_state_id, timestamp)
        VALUES (NEW.id, NEW.job_state_id, datetime('now'));
    END;
    """)

    # Attach the trigger to the database
    event.listen(Base.metadata, 'after_create', trigger)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    states = session.query(JobStates).count()
    if states > 0:
        print("Database has already been initialized. Doing nothing.")

    else:
        states = [
            JobStates(name=JobState.WORKFLOW_STARTED.value, description='Job has been started'),
            JobStates(name=JobState.WORKFLOW_START_FAILED.value, description='Job could not be started'),
            JobStates(name=JobState.WORKFLOW_CHECK_FAILED.value, description='Job check could not be executed'),
            JobStates(name=JobState.WORKFLOW_FINISHED.value, description='Job executing has been finished'),
            JobStates(name=JobState.PUBLISH_FAILED.value, description='Job publish could not be executed'),
            JobStates(name=JobState.PUBLISHED.value, description='Job resulst have been published')
        ]

        session.add_all(states)
        session.commit()
        session.close()


@app.command(name="init")
def init_db(config: Path = typer.Argument(..., help="Path to the YAML configuration file")):
    """Initialize database"""

    config = load_config(config)
    initialize_database(config['database'])


@app.command(name="dump")
def dump_db(db: Path = typer.Argument(..., help="Path to the database to dump")):
    """Dump the contents of database tracking workflow state to stdout"""

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    statement = """
    SELECT
        job.issue,
        job.job_directory,
        project.name AS project,
        job_states.name AS job_state
    FROM
        job
    JOIN
        job_states ON job.job_state_id = job_states.id
    JOIN
        project ON job.project_id = project.id;
    """
    cursor.execute(statement)
    rows = cursor.fetchall()

    conn.close()

    for row in rows:
        print("|".join(row))


class DBConnector:

    def __init__(self, conn_str):
        self.logger = get_logger()
        engine = create_engine(conn_str)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def job_state(self, issue):
        job = self.session.query(Job).filter_by(issue=issue).first()
        if job:
            return job.job_state.name
        return None

    def _check_project(self, project_name):
        # Query to get the Project by name
        project = self.session.query(Project).filter_by(name=project_name).first()

        # Check if the Project was found
        if project:
            return project
        else:
            # If the Project does not exist, create a new one
            project = Project(name=project_name)
            self.session.add(project)
            self.session.commit()  # Commit the new project to the database
            return project

    def start_job(self, issue, job_directory, project):
        start_state = self.session.query(JobStates).filter_by(id=1).first()
        project = self._check_project(project)
        job = Job(issue=issue, job_directory=job_directory, job_state=start_state, project=project)
        self.session.add(job)
        self.session.commit()
        return job

    def transition_job(self, issue, state):
        state = self._check_state(state)
        job = self.session.query(Job).filter_by(issue=issue).first()
        if job:
            job.job_state = self.session.query(JobStates).filter_by(name=state).first()
            self.session.commit()
            return True
        self.logger.error(f"Cannot transition {issue} to {state} -- no job found")
        return False

    def get_jobs(self, state, project=None):
        state = self._check_state(state)
        query = self.session.query(Job).join(JobStates)
        if project is not None:
            query = query.join(Project).filter(Project.name == project)
        query = query.filter(JobStates.name == state)
        jobs = query.all()
        return jobs

    @staticmethod
    def _check_state(state):
        return state.value if isinstance(state, JobState) else state

    def close(self):
        self.session.close()
