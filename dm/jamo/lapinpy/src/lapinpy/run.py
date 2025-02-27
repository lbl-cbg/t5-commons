#!/usr/bin/env python
import argparse
import os
import platform

import daemon.runner as runner

from . import sdmlogger
from .lapinpy_core import RestServer


class SimpleNamespace(object):
    """A SimpleNamespace class to use for Python 2/3 compatibility

    When Python 2 support is no longer needed, it can be replaced with the following line:

    from types import SimpleNamespace
    """
    pass


rootdir = os.path.dirname(os.path.realpath(__file__ + '/..'))
configurations = {}


def get_app(argv, description, daemon=True, epilog=None):
    """Get a python-daemon DaemonRunner for running a daemon process. This
    class expects you to specify the action you want it to take as the first
    argument on the command line. You do not pass in the action as an argument
    to the constructor. This logic is executed in the
    daemon.runner.DaemonRunner.parse_args method. You can override the default
    behavior of pulling the action from the command-line by passing in a
    your own sys.argv into daemon.runner.DaemonRunner.parse_args after calling
    the constructor.

    This function serves as a way of setting up the DaemonRunner so it can be
    called without the DaemonRunner having a dependency on command line
    arguments.
    """
    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument("config", help="The config file to load from")
    parser.add_argument("apps", nargs="*", help="The applications to load")

    args = parser.parse_args(argv)

    working_dir = os.getcwd()

    def run():
        # This needs to be configurable so in case the current working directory is not writeable
        sdmlogger.config(os.path.join(working_dir, os.path.basename(args.config) + ".log"), verbose=True)
        sdmlogger.configQueryLog(os.path.join(working_dir, "query.log"), verbose=True)
        server = RestServer.Instance()
        server.start(args.config, args.apps)

    app = SimpleNamespace()
    app.pidfile_path = f'{os.path.realpath(args.config)}.{platform.node()}.pid'
    app.pidfile_timeout = 10
    app.stdin_path = '/dev/null' if daemon else '/dev/stdin'
    app.stdout_path = os.path.realpath('./stdout')
    app.stderr_path = os.path.realpath('./stderr')
    app.run = run

    return app


def daemon_run(argv, action, description, **kwargs):
    """Get a DaemonRunner and execute the specified action

    Args:
        argv (list)         : a list of arguments to pass through. This will
                              probably be modified sys.argv
        action (str)        : the action for the DaemonRunner to take
        description (str)   : a description to use in the command line usage
                              statement
    """
    app = get_app(argv, description, **kwargs)

    runny = runner.DaemonRunner(app)
    runny.parse_args([None, action])
    runny.daemon_context.working_directory = './'
    runny.do_action()


def start(argv=None):
    """Wrap daemon_run to start a Lapin daemon process"""
    daemon_run(argv, 'start', "Start a Lapin daemon process")


def restart(argv=None):
    """Wrap daemon_run to restart a Lapin daemon process"""
    epi = """
    You must pass in the same config directory used to start the daemon process
    """
    daemon_run(argv, 'restart', "Restart a Lapin daemon process", epilog=epi)


def stop(argv=None):
    """Wrap daemon_run to stop a Lapin daemon process"""
    epi = """
    You must pass in the same config directory used to start the daemon process
    """
    daemon_run(argv, 'stop', "Stop a Lapin daemon process", epilog=epi)


def run_daemonless(argv=None):
    """Run a Lapin not as a daemon process"""
    app = get_app(argv, "Run Lapin not as a daemon process", daemon=False)
    app.run()
