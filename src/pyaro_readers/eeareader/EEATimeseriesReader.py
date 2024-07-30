import csv
from tqdm import tqdm

from geocoder_reverse_natural_earth import (
    Geocoder_Reverse_NE,
    Geocoder_Reverse_Exception,
)
import numpy as np
from pathlib import Path
import polars
import glob
import toml
from pyaro.timeseries import (
    AutoFilterReaderEngine,
    Data,
    Flag,
    NpStructuredData,
    Station,
)

FLAGS_VALID = {-99: False, -1: False, 1: True, 2: False, 3: False, 4: True}
VERIFIED_LVL = [1, 2, 3]
DATA_TOML = "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data.toml"
FILL_COUNTRY_FLAG = False

TS_TYPE_DIFFS = {
    "daily": np.timedelta64(12, "h"),
    "instantaneous": np.timedelta64(0, "s"),
    "points": np.timedelta64(0, "s"),
    "monthly": np.timedelta64(15, "D"),
}


DTYPES = [
    ("values", "f"),
    ("stations", "U64"),
    ("latitudes", "f"),
    ("longitudes", "f"),
    ("altitudes", "f"),
    ("start_times", "datetime64[s]"),
    ("end_times", "datetime64[s]"),
    ("flags", "i2"),
    ("standard_deviations", "f"),
]


PARQUET_FIELDS = dict(
    values="Value",
    stations="stationcode",
    latitudes="lat",
    longitudes="lon",
    altitudes="alt",
    start_times="Start",
    end_times="End",
    flags="Validity",
)


class EEATimeseriesReader(AutoFilterReaderEngine.AutoFilterReader):
    def __init__(
        self,
        filename,
        filters={},
        fill_country_flag: bool = FILL_COUNTRY_FLAG,
    ):
        self._filename = filename
        self._stations = {}
        self._data = {}  # var -> {data-array}

        species = filters["variables"]["include"]
        if len(species) == 0:
            raise ValueError(
                f"As of now, you have to give the species you want to read in filter.variables.include"
            )
        species_ids = self._get_species_ids(species=species)

        filename = Path(filename)
        if not filename.is_dir():
            raise ValueError(
                f"The filename must be an existing path where the data is found in folders with the country code as name"
            )

        for s in species:
            files = self._create_file_list(filename, s)
            lf = polars.scan_parquet(files)
            df = lf.collect()
            length = df.shape[0]

            unit = df.row(0)[df.get_column_index("Unit")]

            array = np.empty(length, np.dtype(DTYPES))
            for key in tqdm(PARQUET_FIELDS):
                print(key)
                array[key] = df[PARQUET_FIELDS[key]].to_numpy()

            data = NpStructuredData(variable=s, units=unit)

            data.set_data(variable=s, units=unit, data=array)
            self._data[s] = data

        # folder_list = self._create_file_list(filename, species_ids)

    def _create_file_list(self, root: Path, species: str):

        results = [f for f in (root / species).glob("**/*.parquet")]
        return results

    def _get_species_ids(self, species: list[str]) -> list[int]:
        ids = []
        with open(DATA_TOML, "r") as f:
            poll = toml.load(f)["pollutant"]
            for key in poll:
                if poll[key] in species:
                    ids.append(key)
        return ids

    def _unfiltered_data(self, varname) -> Data:
        return self._data[varname]

    def _unfiltered_stations(self) -> dict[str, Station]:
        return self._stations

    def _unfiltered_variables(self) -> list[str]:
        return list(self._data.keys())

    def close(self):
        pass


class EEATimeseriesEngine(AutoFilterReaderEngine.AutoFilterEngine):
    def reader_class(self):
        return EEATimeseriesReader

    def open(self, filename, *args, **kwargs) -> EEATimeseriesReader:
        return self.reader_class()(filename, *args, **kwargs)

    def description(self):
        return "Reader for new EEA data API using the pyaro infrastructure."

    def url(self):
        return "https://github.com/metno/pyaro-readers"


if __name__ == "__main__":
    filters = {"variables": {"include": ["PM10"]}}
    EEATimeseriesReader(
        "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/renamed/",
        filters=filters,
    )
