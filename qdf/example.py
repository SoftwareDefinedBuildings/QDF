__author__ = 'immesys'
import qdf
import numpy as np
print ("defining gensin")
class GenSin (qdf.QDF2Distillate):
    def initialize(self, start_t=None, end_t=None):
        self.set_section("Development")
        self.set_name("GenSin")
        self.set_version(3)
        self.register_output("out", "arb_units")

        self.start_t = int(start_t)
        self.end_t   = int(end_t)

    def compute(self, changed_ranges, input_streams, params, report):
        out = report.output("out")

        vals = []
        t = int(self.start_t)
        while t < self.end_t:
            out.addreading(t, np.sin(t/100000000.))
            t += 8333333

        # This specifies a region that should be erased
        # prior to inserting the points that we got
        out.addbounds(self.start_t, self.end_t)
        print "done compute"

class Multiply (qdf.QDF2Distillate):
    def initialize(self, factor=None):
        self.set_section("Development")
        self.set_name("Multiplier")
        self.set_version(2)
        self.register_output("out", "arb_units")
        self.register_input("in")

    def compute(self, changed_ranges, input_streams, params, report):
        out = report.output("out")

        print "compute invoked:"
        print "changed_ranges: ", changed_ranges
        #print "input_streams: ", input_streams
        print "params: ", params

        for r in input_streams["in"]:
            out.addreading(r[0], r[1]*int(params["factor"]))

        # use the changed range as the bounds
        out.addbounds(*changed_ranges["in"])