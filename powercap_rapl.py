import time
import timeit
from multiprocessing import Pipe, Process


class PowercapRapl(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe
        super(PowercapRapl, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)
        stop = False
        energy_uj = []
        start_time = timeit.default_timer()
        while True:
            with open(
                "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj"
            ) as rapl:
                energy_uj.append(int(rapl.read()))
            if stop:
                stop_time = timeit.default_timer()
                self.pipe.send((energy_uj[-1] - energy_uj[0], stop_time - start_time))
                break
            stop = self.pipe.poll(0.1)


class PowercapRaplProfiler:
    def __init__(self):
        self.results = []
        self.total_time = None

    def __enter__(self):
        self.child_conn, self.parent_conn = Pipe()
        p = PowercapRapl(self.child_conn)
        p.start()
        self.parent_conn.recv()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.parent_conn.send(0)
        self.results, self.total_time = self.parent_conn.recv()
        return False


if __name__ == "__main__":
    with PowercapRaplProfiler() as p:
        time.sleep(5)
    print(p.results, p.total_time)
    print("mW: {}".format(p.results / p.total_time / 10 ** 3))
