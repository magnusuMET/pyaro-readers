import unittest
import urllib.request
import os

import pyaro
import pyaro.timeseries
from pyaro.timeseries.Wrappers import VariableNameChangingReader


class TestAERONETTimeSeriesReader(unittest.TestCase):
    engine = "nilupmfebas"

    file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "testdata",
        "PMF_EBAS",
        "SI0008R.20171129230000.20210615130447.low_vol_sampler..pm25.32d.1d.SI01L_ARSO_pm25vz_2.SI01L_ARSO_ECOC_1.lev2.nas",
    )

    testdata_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "testdata", "PMF_EBAS"
    )

    def test_0engine(self):
        self.assertIn(self.engine, pyaro.list_timeseries_engines())

    def test_1open_single_file(self):
        with pyaro.open_timeseries(self.engine, self.file, filters=[]) as ts:
            self.assertGreaterEqual(len(ts.variables()), 1)
            self.assertEqual(len(ts.stations()), 1)

    def test_2open_directory(self):
        with pyaro.open_timeseries(self.engine, self.testdata_dir, filters=[]) as ts:
            self.assertGreaterEqual(len(ts.variables()), 3)
            self.assertEqual(len(ts.stations()), 7)

    # def test_3open_single_file(self):
    #     with pyaro.open_timeseries(self.engine, self.file, filters=[]) as ts:
    #         self.assertGreaterEqual(len(ts.variables()), 1)
    #         self.assertEqual(len(ts.stations()), 1)


if __name__ == "__main__":
    unittest.main()
