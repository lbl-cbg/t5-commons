import argparse

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DDL
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import event

from .utils import load_config
from ..utils import get_logger

# Create a base class for declarative class definitions
Base = declarative_base()

# Define the JobStates table
class JobStates(Base):
    __tablename__ = 'job_states'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    # Relationship to Job
    job = relationship("Job", back_populates="job_state")

# Define the Job table
class Job(Base):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True)
    issue = Column(String, nullable=False)
    job_directory = Column(String, nullable=False)
    job_state_id = Column(Integer, ForeignKey('job_states.id'), nullable=False)

    # Relationship to JobStates
    job_state = relationship("JobStates", back_populates="job")

# Define the JobStateHistory table
class JobStateHistory(Base):
    __tablename__ = 'job_state_history'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('job.id'), nullable=False)
    job_state_id = Column(Integer, ForeignKey('job_states.id'), nullable=False)
    timestamp = Column(String, nullable=False)


def get_session(conn_str):

    return session


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
            JobStates(name='WORKFLOW_STARTED', description='Job has been started'),
            JobStates(name='WORKFLOW_FINISHED', description='Job executing has been finished'),
            JobStates(name='PUBLISH_STARTED', description='Job executing has been finished'),
            JobStates(name='PUBLISHED', description='Job resulst have been published')
        ]

        session.add_all(states)
        session.commit()
        session.close()


def init_db():

    parser = argparse.ArgumentParser(description="Set up a database for a Jira workflow tracker")
    parser.add_argument('config', type=str, help='the config file for the Jira workflow management instance')
    args = parser.parse_args()

    config = load_config(args.config)
    initialize_database(config['database'])


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

    def start_job(self, issue, job_directory):
        self.logger.info(f"Creating new job for {issue}")
        start_state = self.session.query(JobStates).filter_by(name='STARTED').first()
        job = Job(issue=issue, job_directory=job_directory, job_state=start_state)
        self.session.add(job)
        self.session.commit()
        return job

    def transition_job(self, issue, state):
        job = self.session.query(Job).filter_by(issue=issue).first()
        if job:
            requested_state = self.session.query(JobStates).filter_by(name=state).first()
            if requested_state.id > 0:
                previous_state = self.session.query(JobStates).filter_by(id=requested_state.id-1).first()
                if job.job_state is not previous_state:
                    self.logger.error(f"Transition for {issue} ignored. Cannot transition {job.job_state.name} to {state}")
                    return False
            else:
                self.logger.error(f"Transition for {issue} ignored. Cannot transition state to {state}")
                return False
            job.job_state = requested_state
            self.session.commit()
            return True
        self.logger.error(f"Cannot transition {issue} to {state} -- no job found")
        return False

    def close(self):
        self.session.close()
