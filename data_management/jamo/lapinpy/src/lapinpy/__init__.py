from __future__ import print_function
from importlib import import_module


class Command:
    def __init__(self, module, doc):
        ar = ['lapinpy'] + module.split('.')
        self.pkg = '.'.join(ar[:-1])
        self.func = ar[-1]
        self.doc = doc

    def get_func(self):
        return getattr(import_module(self.pkg), self.func)


def main():
    """A function for the master LapinPy command"""
    command_dict = {
        'Setting up LapinPy': {
            'init': Command('init.init', 'Set up core database needed for running LapinPy')
        },
        'Running LapinPy': {
            'start': Command('run.start', 'Start a LapinPy daemon process'),
            'stop': Command('run.stop', 'Stop a LapinPy daemon process'),
            'restart': Command('run.restart', 'Restart a LapinPy daemon process'),
            'run': Command('run.run_daemonless', 'Run a LapinPy process not as a daemon'),
        },
    }
    import sys
    if len(sys.argv) == 1:
        print('Usage: %s <command> [options]' % sys.argv[0].split('/')[-1])
        print('Available commands are:\n')
        for g, d in command_dict.items():
            print(' ' + g)
            for c, f in d.items():
                print('    ' + c.ljust(16) + f.doc)
            print()
    else:
        cmd = sys.argv[1]
        for g, d in command_dict.items():
            func = d.get(cmd)
            if func is not None:
                func = func.get_func()
                break
        if func is not None:
            argv = sys.argv[2:]
            sys.argv[0] = sys.argv[0] + " " + sys.argv[1]
            func(argv)
        else:
            print("Unrecognized command: '%s'" % cmd, file=sys.stderr)
