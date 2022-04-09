import plistlib
import subprocess
import sys
import time
from multiprocessing import Pipe, Process

import pandas as pd




class PowerMetrics(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe

        super(PowerMetrics, self).__init__(*args, **kw)

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
            stop = self.pipe.poll(0.05)
        self.pipe.send(buffer)


def powermetrics(proc):
    if callable(proc):
        proc = (proc, (), {})
    if isinstance(proc, (list, tuple)):
        if len(proc) == 1:
            f, args, kw = (proc[0], (), {})
        elif len(proc) == 2:
            f, args, kw = (proc[0], proc[1], {})
        elif len(proc) == 3:
            f, args, kw = (proc[0], proc[1], proc[2])
        else:
            raise ValueError
    child_conn, parent_conn = Pipe()
    p = PowerMetrics(child_conn)
    p.start()
    
    parent_conn.recv()
    proc_output = f(*args, **kw)
    parent_conn.send(0)

    outputs = parent_conn.recv()
    result = pd.json_normalize(outputs, ["processor", "clusters"], "timestamp")
    return proc_output, result


if __name__ == "__main__":
    def test():
        time.sleep(20)
        return None
    result = powermetrics(test)
    print(result)
