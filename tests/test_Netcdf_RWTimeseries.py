import logging
import os
from shutil import rmtree
import sys
import unittest
import numpy as np

import pyaro
import pyaro.timeseries

EBAS_URL = file = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "testdata", "NILU"
)


class TestNetcdf_RWTimeSeries(unittest.TestCase):
    engine = "ascii2netcdf"
    rwengine = "netcdf_rw"
    rwdir = "tmp_netcdf_rw"

    def setUp(self) -> None:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)

        os.makedirs(self.rwdir, exist_ok=True)
        return super().setUp()

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(cls.rwdir):
            rmtree(cls.rwdir)
            pass
        return super().tearDownClass()

    def test_0engine(self):
        self.assertIn(self.rwengine, pyaro.list_timeseries_engines())

    def test_1write(self):
        with pyaro.open_timeseries(
            self.rwengine, self.rwdir, mode="w", filters=[]
        ) as ts_rw:
            with pyaro.open_timeseries(
                self.engine, EBAS_URL, resolution="daily", filters=[]
            ) as ts:
                self.assertGreater(len(ts.variables()), 70)
                self.assertGreater(len(ts.stations()), 300)
                ts_rw.add(ts)
                self.assertEqual(len(ts.variables()), len(ts_rw.variables()))
                self.assertEqual(len(ts.stations()), len(ts_rw.stations()))

    def test_2open(self):
        with pyaro.open_timeseries(
            self.rwengine, self.rwdir, mode="w", filters=[]
        ) as ts_rw:
            with pyaro.open_timeseries(
                self.engine, EBAS_URL, resolution="daily", filters=[]
            ) as ts:
                self.assertEqual(len(ts.variables()), len(ts_rw.variables()))
                self.assertEqual(len(ts.stations()), len(ts_rw.stations()))

    def test_3write(self):
        # write same data again, should not increase
        with pyaro.open_timeseries(
            self.rwengine, self.rwdir, mode="w", filters=[]
        ) as ts_rw:
            with pyaro.open_timeseries(
                self.engine, EBAS_URL, resolution="daily", filters=[]
            ) as ts:
                self.assertGreater(len(ts.variables()), 70)
                self.assertGreater(len(ts.stations()), 300)
                ts_rw.add(ts)
                self.assertEqual(len(ts.variables()), len(ts_rw.variables()))
                self.assertEqual(len(ts.stations()), len(ts_rw.stations()))
