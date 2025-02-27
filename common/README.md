# The t5common package
`t5common` is a Python package for common operations in the Taskforce5 commons.

- A class for connecting to and interacting with Jira (Python class `t5common.jira.JiraConnector`)
- A framework for polling Jira for new issues, and starting workflows (commands `init-db`, `check-jira`, and `mark-job`)
- A class for building Slurm sbatch scripts (Python class `t5common.job.SlurmJob`)


## Jira-based workflow automation

Workflows can be automatically triggered using the `init-db`, `check-jira`, and `mark-job` commands that come with
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
   - `publish_command` - The command to run to publish results. This command should take working directory the workflow was started from as the first and only positional argument.

For more details, refer to the JSON schema in`t5common/jira/schema/config.json`.


### Initializing workflow automation

Once you have defined your configuration file, you will need to initialize the SQLite database using the `init-db` command.

```bash
init-db config.yaml
```

This database maintain jobs in three states:

- `WORKFLOW_STARTED` - Job has been picked up from Jira and workflow has been started
- `WORKFLOW_FINISHED` - Job execution has finished
- `PUBLISH_STARTED` - Result publishing script has been started
- `PUBLISHED` - Job results have been published


### Starting and checking on jobs

**This section documents the commands that must run as a cron job to automate workflow execution**

Jobs can be started using the `check-jira` command.

```bash
check-jira config.yaml
```

This will check each project specified in with the `projects` key the configuration file, and start job for each new issue. This
job will be started by invoking the command specified with `workflow_command` with the issue provided as the first and only argument.
The `workflow_command` command will be invoked from a subdirectory named after the issue key and created in the directory specified 
by the `job_directory` key of the configuration file. 

Job status can be checked using the `check-job` command.

```bash
check-jobs config.yaml
```

This will check the database for jobs that have been marked as finished (See below for `mark-job` command). The `check-jobs` command
will run the command specified with the `publish_command` in the config file, passing in the path to subdirectory that the workflow 
was executed from as the first and only argument.


### Updating jobs

**This section documents the command that workflows are required to use to connect to the workflow automation system**

Workflows will need to indicate that jobs have been finished or published using the command `mark-job`. The first argument to this command
must be `finished` or `published`, indicating that the job has finished running or the results have been published, respectively. The `mark-job`
command also takes a second optional argument, specifying the job directory. This defaults to the current working directory. 

```bash
mark-job finished
```

Workflows must ensure that the `t5common` package is installed in their environments, and that they call `mark-job` when steps are complete.
