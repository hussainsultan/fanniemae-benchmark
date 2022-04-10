import plistlib
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from multiprocessing import Pipe, Process


class PowerMetricsDetailProcess(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe

        super(PowerMetricsDetailProcess, self).__init__(*args, **kw)

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


class PowerMetricsSummaryProcess(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe

        super(PowerMetricsSummaryProcess, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)
        stop = False
        buffer = []
        cmd_args = [
            "powermetrics",
            "-f",
            "plist",
            "-s",
            "cpu_power",
            "-i",
            "0",
            "--show-usage-summary",
        ]
        process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
        while True:
            if stop:
                process.send_signal(signal.SIGINT)
                buffer.append(plistlib.loads(process.stdout.read()))
                break
            stop = self.pipe.poll(0.1)
        self.pipe.send(buffer)


class PowerMetricsProfiler:
    def __init__(self):
        self.results = []

    def __enter__(self):
        self.child_conn, self.parent_conn = Pipe()
        p = PowerMetricsSummaryProcess(self.child_conn)
        p.start()
        self.parent_conn.recv()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.parent_conn.send(0)
        self.results = self.parent_conn.recv()
        return False


class PowerMetricsDetailProfiler:
    def __init__(self):
        self.results = []

    def __enter__(self):
        self.child_conn, self.parent_conn = Pipe()
        p = PowerMetricsDetailProcess(self.child_conn)
        p.start()
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
