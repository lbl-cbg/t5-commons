
from .job import AbstractJob

class SlurmJob(AbstractJob):

    directive = 'SBATCH'
    queue_flag = 'q'
    wait_flag = 'd'
    project_flag = 'A'
    time_flag = 't'
    output_flag = 'o'
    error_flag = 'e'
    jobname_flag = 'J'
    nodes_flag = '-ntasks'
    submit_cmd = 'sbatch'
    job_var = 'SLURM_JOB_ID'
    job_fmt_var = 'j'
    job_id_re = 'Submitted batch job (\d+)'

    debug_queue = 'debug'

    def __init__(self, queue='regular', project=None, time='1:00:00', nodes=1, gpus=0, jobname=None, output=None, error=None, wait=None):
        super().__init__(queue, project, time, nodes, gpus, jobname, output, error, wait=wait)
        self.queue = queue
        self.project = project
        self.time = time
        self.jobname = jobname
        if self.jobname is not None:
            self.output = f'{self.jobname}.%J'
            self.error = f'{self.jobname}.%J'

        if self.gpus > 0:
            self.nodes = self.gpus * nodes # nodes will actually be the flag for Total number of tasks
            self.add_addl_jobflag('C', 'gpu')
            self.add_addl_jobflag('-ntasks-per-node', self.gpus)
            self.add_addl_jobflag('-gpus-per-node', self.gpus)
        else:
            self.nodes = nodes # nodes will actually be the flag for Total number of tasks
            self.add_addl_jobflag('C', 'cpu')

    def write_run(self, f, command, command_options, options):
        print(f'srun -u {command}', file=f)
