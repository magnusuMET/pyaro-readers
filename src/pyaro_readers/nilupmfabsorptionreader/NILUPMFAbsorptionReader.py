import csv
from io import BytesIO
from urllib.parse import urlparse
from urllib.request import urlopen
from zipfile import BadZipFile, ZipFile
from pathlib import Path
import datetime

from geocoder_reverse_natural_earth import Geocoder_Reverse_NE

import numpy as np
import requests
from pyaro.timeseries import (
    AutoFilterReaderEngine,
    Data,
    Flag,
    NpStructuredData,
    Station,
)
from tqdm import tqdm

# default URL
# BASE_URL = "https://secondary-data-archive.nilu.no/ebas/gen.h8ds-8596/EIMPs_winter2017-2018_data.zip"
BASE_URL = "/lustre/storeB/project/fou/kl/emep/People/danielh/projects/pyaerocom/obs/nilu_pmf/cameo_2024/EIMPs_winter2017-2018_data/"
ABSORB_FOLDER = "EIMPs_winter_2017_2018_absorption/"
LEVO_FOLDER = "EIMPs_winter_2017_2018_ECOC_Levo/"
METADATA_FILE = "Sites_EBC-campaign.xlsx"
# number of lines to read before the reading is handed to Pythobn's csv reader
HEADER_LINE_NO = 7
DELIMITER = ","
#
NAN_VAL = -999.0
# update progress bar every N lines...
PG_UPDATE_LINES = 100
# main variables to store
LAT_NAME = "Station latitude"
LON_NAME = "Station longitude"
ALT_NAME = "Station altitude"
STAT_CODE = "Station code"
STAT_NAME = "Station name"
DATE_NAME = "Date(dd:mm:yyyy)"
TIME_NAME: str = "Time(hh:mm:ss)"

BABAS_BB_NAME = "Babs_bb"
BABAS_FF_NAME = "Babs_ff"
EBC_BB_NAME = "eBC_bb"
EBC_FF_NAME = "eBC_ff"


NAN_CODE = 999.9999
NAN_EPS = 1e-2


INDECIES = dict(
    PI=1,
    DATES=6,
    INTERVAL_DAYS=7,
    BABAS_BB_UNIT=13,
    BABAS_FF_UNIT=14,
    EBC_BB_UNIT=15,
    EBC_FF_UNIT=16,
    START=17,
    CODE=18,
    NAME=19,
    LAT=20,
    LON=21,
    ALT=22,
)


DATA_VARS = [BABAS_BB_NAME, BABAS_FF_NAME, EBC_BB_NAME, EBC_FF_NAME]
COMPUTED_VARS = []
# The computed variables have to be named after the read ones, otherwise the calculation will fail!
DATA_VARS.extend(COMPUTED_VARS)

FILL_COUNTRY_FLAG = False

TS_TYPE_DIFFS = {
    "daily": np.timedelta64(12, "h"),
    "instantaneous": np.timedelta64(0, "s"),
    "points": np.timedelta64(0, "s"),
    "monthly": np.timedelta64(15, "D"),
}


class NILUPMFAbsorptionReader(AutoFilterReaderEngine.AutoFilterReader):
    def __init__(
        self,
        filename,
        filters=[],
        fill_country_flag: bool = FILL_COUNTRY_FLAG,
        tqdm_desc: [str, None] = None,
        ts_type: str = "hourly",
    ):
        self._stations = {}
        self._data = {}
        self._set_filters(filters)
        self._header = []

        if Path(filename).is_file():
            self._filename = filename
            self._process_file(self._filename)

        elif Path(filename).is_dir():
            self._filename = filename + ABSORB_FOLDER
            files_pathlib = Path(self._filename).glob("*.nas")
            files = [x for x in files_pathlib if x.is_file()]

            if len(files) == 0:
                raise ValueError(
                    f"Could not find any nas files in given folder {self._filename}"
                )
            bar = tqdm(desc=tqdm_desc, total=len(files))
            for file in files:
                bar.update(1)
                self._process_file(file)
        else:
            raise ValueError(f"Given filename {filename} is neither a folder or a file")

    def _process_file(self, file: Path):
        with open(file, newline="") as f:
            lines = f.readlines()
            self._process_open_file(lines, file)

    def _process_open_file(self, lines: list[str], file: Path) -> None:
        line_index = 0
        data_start_line = int(lines[line_index].split()[0])
        station = lines[INDECIES["CODE"]].split(":")[1].strip()
        long_name = lines[INDECIES["NAME"]].split(":")[1].strip()

        station = long_name

        startdate = "".join(lines[INDECIES["DATES"]].split()[:3])
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

        lon = float(lines[INDECIES["LON"]].split(":")[1].strip())
        lat = float(lines[INDECIES["LAT"]].split(":")[1].strip())
        alt = float(lines[INDECIES["ALT"]].split(":")[1].strip()[:-1])
        print(station)
        if not station in self._stations:
            self._stations[station] = Station(
                {
                    "station": station,
                    "longitude": lon,
                    "latitude": lat,
                    "altitude": alt,
                    "country": self._lookup_function()(lat, lon),
                    "url": str(file),
                    "long_name": station,
                }
            )

        units = {
            BABAS_BB_NAME: lines[INDECIES["BABAS_BB_UNIT"]].split(",")[1].strip(),
            BABAS_FF_NAME: lines[INDECIES["BABAS_FF_UNIT"]].split(",")[1].strip(),
            EBC_BB_NAME: lines[INDECIES["EBC_BB_UNIT"]].split(",")[1].strip(),
            EBC_FF_NAME: lines[INDECIES["EBC_FF_UNIT"]].split(",")[1].strip(),
        }
        data_index_list = lines[data_start_line - 1].split()
        data_indecies = {
            BABAS_BB_NAME: data_index_list.index(BABAS_BB_NAME),
            BABAS_FF_NAME: data_index_list.index(BABAS_FF_NAME),
            EBC_BB_NAME: data_index_list.index(EBC_BB_NAME),
            EBC_FF_NAME: data_index_list.index(EBC_FF_NAME),
        }
        for variable in DATA_VARS:
            if variable in self._data:
                da = self._data[variable]
                if da.units != units[variable]:
                    raise Exception(
                        f"unit change from '{da.units}' to {units[variable]}"
                    )
            else:
                da = NpStructuredData(variable, units[variable])
                self._data[variable] = da

        for line in lines[data_start_line:]:
            line_entries = [
                float(x) if abs(float(x) - NAN_CODE) > NAN_EPS else np.nan
                for x in line.split()
            ]
            starttime = startdate + datetime.timedelta(
                hours=int(line_entries[0] * 24)
            )  # startdate + datetime.timedelta(hours=line_entries[0])
            endtime = startdate + datetime.timedelta(hours=int(line_entries[0] * 24))

            for key in data_indecies:
                value = line_entries[data_indecies[key]]
                flag = Flag.VALID if ~np.isnan(value) else Flag.INVALID
                self._data[key].append(
                    value,
                    station,
                    lat,
                    lon,
                    alt,
                    starttime,
                    endtime,
                    flag,
                    np.nan,
                )

    def _unfiltered_data(self, varname) -> Data:
        return self._data[varname]

    def _unfiltered_stations(self) -> dict[str, Station]:
        return self._stations

    def _unfiltered_variables(self) -> list[str]:
        return list(self._data.keys())

    def close(self):
        pass

    def is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def _lookup_function(self):
        geo = Geocoder_Reverse_NE()
        return lambda lat, lon: geo.lookup_nearest(lat, lon)["ISO_A2_EH"]


class NILUPMFAbsorptionTimeseriesEngine(AutoFilterReaderEngine.AutoFilterEngine):
    def reader_class(self):
        return NILUPMFAbsorptionReader

    def open(self, filename, *args, **kwargs) -> NILUPMFAbsorptionReader:
        return self.reader_class()(filename, *args, **kwargs)

    def description(self):
        return "Simple reader of Nilu PMF-files using the pyaro infrastructure"

    def url(self):
        return "https://github.com/metno/pyaro-readers"


if __name__ == "__main__":
    file_name = "/lustre/storeB/project/fou/kl/emep/People/danielh/projects/pyaerocom/obs/nilu_pmf/cameo_2024/EIMPs_winter2017-2018_data/"
    reader = NILUPMFAbsorptionReader(filename=file_name)
