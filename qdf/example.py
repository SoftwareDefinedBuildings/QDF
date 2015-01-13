__author__ = 'immesys'
import qdf
import numpy as np
print ("defining gensin")
class GenSin (qdf.QDF2Distillate):
    def __init__(self, start_t=None, end_t=None):
        self.superinit()
        self.set_category("Development")
        self.set_name("GenSin")
        self.set_version(1)
        self.register_output("out", "arb_units")

        self.start_t = int(start_t)
        self.end_t   = int(end_t)

    def compute(self, changed_ranges, input_streams, params, report):
        out = report.output("out")

        vals = []
        t = int(self.start_t)
        while t < self.end_t:
            out.addreading(t, np.sin(t/10))
            t += 10

        # This specifies a region that should be erased
        # prior to inserting the points that we got
        out.addbounds(self.start_t, self.end_t)

        return report
