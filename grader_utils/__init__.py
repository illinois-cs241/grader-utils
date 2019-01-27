"""Basic wrappers around subprocess.Popen for autograding purposes."""
import time
from threading import Thread
from subprocess import PIPE, Popen
import os
import signal
import shlex
import logging

# File handler to /dev/null or equivalent
DEVNULL = open(os.devnull, 'wb')

lg = logging.getLogger("process_wrapper")

class ProcessWrapper(object):
    """Wrapper class around Popen that allows timeout of long running code and
    other features.

    Note that by default, the ProcessWrapper constructor has "normal" defaults
    that may not be ideal for running code. This includes not capturing
    stdout, stderr, pipe for stdin, and no timeout.
    """
    TIME_UNTIL_SIGKILL_SECS = 1

    def __init__(self, command, **kwargs):
        """Construct ProcessWrapper object.

        command: list of strings that form the command and its arguments that
            should be executed. Can also be a string, in which case
            shlex.split() will parse it using shell-like syntax.

        kwargs args:
        :param int timeout: How many seconds to wait until sending SIGTERM to process.
            Note that if the SIGTERM fails, SIGKILL will be sent soon
            afterwards. Default is None.
        :param file stdout: Specify file handler for process's stdout. Use PIPE to get
            stdout from self.join(). Use DEVNULL to discard stdout. Use None to
            inherit stdout from parent. Default is DEVNULL.
        :param file stderr: Same as stdout but for stderr.
        :param file stdin: Same as stdout but for stdin. Note that input and delayed_inputs
            will only work if this is set to PIPE.
        :param str cwd: Set the directory for the process.
        :param str input: String to feed in for stdin. Only works if stdin is set to PIPE.
            Set to None to not feed in anything. Default is None.
        :param list[str] | list[int] delayed_inputs: Will iterate through list and for
            each entry input the string into
            stdin or wait the given amount of time. Default is [].
        :param bool check_pg_alive: Indicate whether to use our is_alive() or
            Thread.is_alive() in the join() function. Default is True since we
            want to check for running child processes after the main process
            exits.
        """

        super(ProcessWrapper, self).__init__()
        self.command = (shlex.split(command) if type(command) is str
                        else command)
        self.timeout = kwargs.get('timeout', None)
        self.stdout = kwargs.get('stdout', DEVNULL)
        self.stderr = kwargs.get('stderr', DEVNULL)
        self.stdin = kwargs.get('stdin', DEVNULL)
        self.cwd = kwargs.get('cwd', None)
        self.inputs = kwargs.get('input', None)
        self.delayed_inputs = kwargs.get('delayed_inputs', [])
        self.disable_communicate = kwargs.get('disable_communicate', False)
        self.check_pg_alive = kwargs.get('check_pg_alive', True)
        self.env = kwargs.get('env', None)

        # Keep track of subprocess and thread that waits on it
        self.process = None
        self.thread = None

        # Fields that will be outputted later
        self.killed = False
        self.stdout_res = None
        self.stderr_res = None
        # If the process timeouts
        self.exit_timeout = False
        # If the process is killed by SIGKILL or exit normally
        self.exit_force = False
        # If
        # - the process timeouts,
        # - or any of the processes in the process group timeouts while main
        #   process exited.
        # Either case we will try to kill the process group using SIGTERM.
        self.exit_timeout_pg = False
        # If
        # - the process is not killed by SIGTERM
        # - or any of the processes in the process group is not killed by
        #   SIGTERM.
        # In either case we will try to kill the process group using SIGKILL.
        self.exit_force_pg = False

    def run(self):
        """Run the command as a separate process and return immediately.

        Call join() later to get result of process. Timeout does not start
        until join() is called. Additionally send the delayed_inputs and inputs
        to process.
        """
        def target():
            # Pass these inputs to STDIN with delays
            for i in self.delayed_inputs:
                if type(i) is int or type(i) is float:
                    time.sleep(i)
                elif type(i) is bytes:
                    try:
                        self.process.stdin.write(i) 
                    except IOError as e:
                        lg.info(
                            "Input: {} failed to write to stdin due to\n{}".format(i, e)
                            )
                        break
            if self.disable_communicate:
                self.process.wait()
            else:
                self.stdout_res, self.stderr_res = self.process.communicate(
                    input=self.inputs)

        try:
            self.process = Popen(self.command, stdin=self.stdin,
                                 stdout=self.stdout, stderr=self.stderr,
                                 start_new_session=True, cwd=self.cwd, env=self.env)
        except OSError:
            lg.error("Couldn't Popen command {}".format(self.command))
            raise
        self.thread = Thread(target=target)
        self.thread.start()

    def join(self):
        """Join with the thread and get the result of the separate process.

        Will timeout based on self.timeout. Timeout involves sending SIGTERM
        followed by SIGKILL if that is ignored.
        """
        self.thread.join(timeout=self.timeout)
        alive = self.is_alive if self.check_pg_alive else self.thread.is_alive
        self.exit_timeout = self.thread.is_alive()
        if alive():
            self.exit_timeout_pg = self.check_pg_alive
            lg.info("{}: is still alive. Terminating now...".format(self.process.pid))
            os.killpg(self.process.pid, signal.SIGTERM)
            self.thread.join(timeout=ProcessWrapper.TIME_UNTIL_SIGKILL_SECS)
            self.exit_force = self.thread.is_alive()
            if alive():
                self.exit_force_pg = self.check_pg_alive
                lg.warning(
                    "{}: is still alive after SIGTERM. Pressing the big red button...".format(self.process.pid)
                    )
                os.killpg(self.process.pid, signal.SIGKILL)
                if self.thread.is_alive():
                    lg.error(
                        "{self.process.pid}: is still alive after SIGKILL. May God have mercy on our souls...".format(self.process.pid)
                        )
        return {
            'returncode': self.process.returncode,
            'stdout': self.stdout_res,
            'stderr': self.stderr_res,
            'timeout': self.exit_timeout,
            'killed': self.exit_force,
            'timeout_pg': self.exit_timeout_pg,
            'killed_pg': self.exit_force_pg,
        }

    def send_signal(self, sig):
        """
        Send a signal to this process.
        :param int | signal.SIGNAL sig: signal to deliver
        """
        if self.thread.is_alive():
            self.process.send_signal(sig)
        else:
            lg.warning("Couldn't deliver signal "
                       "to already dead process {} ({})".format(self.command[0], self.process.pd)
                       )

    def is_alive(self):
        """
        Check if the process group of the executed process is still alive.
        :return: bool if is alive
        """
        result = execute('ps -Ao pgid', check_pg_alive=False, stdout=PIPE)
        pgids = result['stdout'].decode('utf8').split()
        return str(self.process.pid) in pgids


def execute(command, **kwargs):
    """Start and wait for a ProcessWrapper.

    command: List of strings (command/arguments) or a single string to run in a
        subprocess
    kwargs: kwargs directly passed to ProcessWrapper.__init__()
    :return: dict containing results of the process
    """
    proc = ProcessWrapper(command, **kwargs)
    proc.run()
    return proc.join()
