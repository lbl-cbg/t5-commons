# The t5common package
`t5common` is a Python package for common operations in the Taskforce5 commons.

- A class for connecting to and interacting with Jira (Python class `t5common.jira.JiraConnector`)
- A framework for polling Jira for new issues, and starting workflows (accessible through the `t5 jwf` command)
- A class for building Slurm sbatch scripts (Python class `t5common.job.SlurmJob`)


## Jira-based workflow automation

Workflows can be automatically triggered using the command suite available with the `t5 jwf` command that comes with
the `t5common` package. 

### Configuring workflow automation

Workflow automation with Jira is configured using a YAML file. The YAML file must contain the following keys:

- `host` - the Jira host to get issues for running jobs from
- `user` - the username for connecting to Jira
- `token_file` - the path to a file containing the Jira API token. This file should contain the token on a single line
- `database` - the path to the SQLite database to use for tracking jobs associated with issues
- `job_directory` - the job directory to run jobs from.
- `projects` - A list of objects containing the information needed to automate workflows from a Jira project. These objects must contain the following keys:
   - `project` - The project to query new issues for
   - `new_status` - The issue status indicating an issue is new and should have a workflow run for it.
   - `workflow_command` - The command to run to start a new workflow. This command should take the issue key as the first and only positional argument.
   - `check_command` - The command to run to check the state of a running workflow. This command should take the working directory that the workflow was started from as the first and only positional argument.
   - `publish_command` - The command to run to publish results. This command should take the working directory that the workflow was started from as the first and only positional argument.

For more details, refer to the JSON schema in`t5common/jira/schema/config.json`.


### Initializing workflow automation

Once you have defined your configuration file, you will need to initialize the SQLite database using the `db init` subcommand.

```bash
t5 jwf db init config.yaml
```

This database maintain jobs in three states:

- `WORKFLOW_STARTED` - Job has been picked up from Jira and workflow has been started
- `WORKFLOW_FINISHED` - Job execution has finished
- `PUBLISHED` - Job results have been published

Additionally, it has four error states, indicating non-zero exits from respective commands:

- `WORKFLOW_START_FAILED` - Job start command failed
- `WORKFLOW_FAILED` - Job workflow failed
- `WORKFLOW_CHECK_FAILED` - Job check command failed
- `PUBLISH_FAILED` - Job publishing command failed


### Starting and checking on jobs

**This section documents the commands that must run as a cron job to automate workflow execution**

Jobs can be started using the `start` subcommand.

```bash
t5 jwf start config.yaml
```

This will check each project specified in with the `projects` key the configuration file, and start job for each new issue. This
job will be started by invoking the command specified with `workflow_command` with the issue provided as the first and only argument.
The `workflow_command` command will be invoked from a subdirectory named after the issue key and created in the directory specified 
by the `job_directory` key of the configuration file. 

Workflow status can be checked using the `check` subcommand.

```bash
t5 jwf check config.yaml
```

This will check the database for jobs that have been started. The `check` subcommand
will run the command specified with the `check_command` in the config file, passing in the path to subdirectory that the workflow 
was executed from as the first and only argument. The command should call the `mark-job` subcommand to indicate any changes (i.e.
failure or completion) as appropriate. 

Workflow results can be published using the `publish` subcommand.

```bash
t5 jwf publish config.yaml
```

This will check the database for jobs that have been marked as finished (See below for `mark-job` command), and run the command 
specified with the `publish_command` in the config file, passing in the path to subdirectory that the workflow was executed from
as the first and only argument.


### Updating jobs

**This section documents the command that workflows are required to use to connect to the workflow automation system**

Workflows will need to indicate that workflows have been finished or published using the subcommand `mark-job`. The first argument to this command
must be one of the database states listed above. Although `mark-job` can take any of the seven states, users should only be passing one of
`WORKFLOW_FINISHED`, `PUBLISHED`, and `WORKFLOW_FAILED`. _Passing other states may have unintended effects and should be done with caution!_
The `mark-job` subcommand also takes a second optional argument, specifying the job directory. This defaults to the current working directory.

```bash
t5 jwf mark-job WORKFLOW_FINISHED
```

Workflows must ensure that the `t5common` package is installed in their environments, and that they call `mark-job` when steps are complete.
