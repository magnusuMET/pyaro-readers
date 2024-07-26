import csv


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


class EEATimeseriesReader(AutoFilterReaderEngine.AutoFilterReader):
    def __init__(
        self,
        filename,
        filters={},
        fill_country_flag: bool = FILL_COUNTRY_FLAG,
    ):
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
        files = self._create_file_list(filename, species)
        lf = polars.scan_parquet(files)
        df = lf.collect()
        df = df.with_columns(
            (polars.col("Samplingpoint").str.extract(r"(.*)/.*")).alias("Country Code")
        )

        return df
        # folder_list = self._create_file_list(filename, species_ids)

    def _create_file_list(self, root: Path, species: list[str]):
        results = []
        for s in species:
            results += [f for f in (root / s).glob("**/*.parquet")]
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

    def is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False


# class AeronetSunTimeseriesEngine(AutoFilterReaderEngine.AutoFilterEngine):
#     def reader_class(self):
#         return AeronetSunTimeseriesReader

#     def open(self, filename, *args, **kwargs) -> AeronetSunTimeseriesReader:
#         return self.reader_class()(filename, *args, **kwargs)

#     def description(self):
#         return "Simple reader of AeronetSun-files using the pyaro infrastructure"

#     def url(self):
#         return "https://github.com/metno/pyaro-readers"


if __name__ == "__main__":
    filters = {"variables": {"include": ["SO2", "PM10", "PM2.5"]}}
    reader = EEATimeseriesReader(
        "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data",
        filters=filters,
    )
