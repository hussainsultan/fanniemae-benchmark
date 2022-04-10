import plistlib
import subprocess
import sys
import time
from contextlib import contextmanager
from multiprocessing import Pipe, Process


class PowerMetricsProcess(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe

        super(PowerMetricsProcess, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)
        stop = False
        buffer = []
        while True:
            cmd_args = [
                "powermetrics",
                "-f",
                "plist",
                "-s",
                "cpu_power",
                "-i",
                "1",
                "-n",
                "1",
            ]
            process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
            process.wait()
            buffer.append(plistlib.loads(process.stdout.read()))
            if stop:
                break
            stop = self.pipe.poll(0.1)
        self.pipe.send(buffer)


class PowerMetricsProfiler:
    def __init__(self):
        self.results = []

    def __enter__(self):
        self.child_conn, self.parent_conn = Pipe()
        self.p = PowerMetricsProcess(self.child_conn)
        self.p.start()
        self.parent_conn.recv()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.parent_conn.send(0)
        self.results = self.parent_conn.recv()
        return False


if __name__ == "__main__":
    with PowerMetricsProfiler() as p:
        time.sleep(20)

    print(p.results)
