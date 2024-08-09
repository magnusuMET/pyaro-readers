import csv
from tqdm import tqdm
from datetime import datetime

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
    Filter,
    Flag,
    NpStructuredData,
    Station,
)

FLAGS_VALID = {-99: False, -1: False, 1: True, 2: False, 3: False, 4: True}
VERIFIED_LVL = [1, 2, 3]
DATA_TOML = "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data.toml"
FILL_COUNTRY_FLAG = False

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

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
    #countries="country",
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
        self._set_filters(filters)

        species = filters["variables"]["include"]

        filter_time = False
        if "time_bounds" in filters:
            if "start_include" in filters["time_bounds"]:
                start_date = datetime.strptime(filters["time_bounds"]["start_include"][0][0], TIME_FORMAT)
                end_date = datetime.strptime(filters["time_bounds"]["start_include"][0][1], TIME_FORMAT)
                filter_time = True


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
            if filter_time:
                datapoints = self._filter_dates(polars.scan_parquet(files), (start_date, end_date)).select(polars.len()).collect()[0, 0]
            else:
                datapoints = polars.scan_parquet(files).select(polars.len()).collect()[0, 0]


            array = np.empty(datapoints, np.dtype(DTYPES))

            data = None
            species_unit = None

            current_idx = 0

            # if filter_time:
            #     df = self._filter_dates(polars.scan_parquet(files), (start_date, end_date)).collect()
            # else:
            #     df = polars.scan_parquet(files).collect()

            # for key in tqdm(PARQUET_FIELDS):
            #         array[key] = df.get_column(PARQUET_FIELDS[key]).to_numpy()



            for file in tqdm(files):
                
                if filter_time:
                    lf = self._filter_dates(polars.read_parquet(file), (start_date, end_date))
                    if lf.is_empty():
                        #print(f"Empty filter for {file}")
                        continue
                else:
                    lf = polars.read_parquet(file)


                file_datapoints = lf.select(polars.len())[0,0]#.collect()
                df = lf#.collect()

                file_unit = df.row(0)[df.get_column_index("Unit")]

                for key in PARQUET_FIELDS:
                    array[key][current_idx : current_idx + file_datapoints] = df.get_column(PARQUET_FIELDS[key]).to_numpy()

                current_idx += file_datapoints

                if species_unit is None:
                    species_unit = file_unit
                else:
                    if species_unit != file_unit:
                        raise ValueError(
                            f"Found multiple units ({file_unit} and {species_unit}) for same species {s}"
                        )
                    

                metadatarow = df.row(0)
                station_fields = {
                    "station": metadatarow[df.get_column_index(PARQUET_FIELDS["stations"])],
                    "longitude": metadatarow[df.get_column_index(PARQUET_FIELDS["longitudes"])],
                    "latitude": metadatarow[df.get_column_index(PARQUET_FIELDS["latitudes"])],
                    "altitude": metadatarow[df.get_column_index(PARQUET_FIELDS["altitudes"])],
                    "country": metadatarow[df.get_column_index("country")],
                    "url": "",
                    "long_name": metadatarow[df.get_column_index(PARQUET_FIELDS["stations"])],
                }
                self._stations[metadatarow[df.get_column_index(PARQUET_FIELDS["stations"])]] = Station(station_fields)
                

            data = NpStructuredData(variable=s, units=species_unit)
            data.set_data(variable=s, units=species_unit, data=array)
            self._data[s] = data

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
    
    def _filter_dates(self, lf: polars.LazyFrame | polars.DataFrame, dates: tuple[datetime]) -> polars.LazyFrame | polars.DataFrame:
        if dates[0] >= dates[1]:
            raise ValueError(f"Error when filtering data. Last date {dates[1]} must be larger than the first {dates[0]}")
        #return lf.with_columns(polars.col(PARQUET_FIELDS["start_times"]).str.strptime(polars.Date)).filter(polars.col(PARQUET_FIELDS["start_times"]).is_between(dates[0], dates[1]))
        return lf.filter(polars.col(PARQUET_FIELDS["start_times"]).is_between(dates[0], dates[1]))

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
    filters = {"variables": {"include": ["PM10"]}, "time": {"start": "2018-01-01", "stop": "2018-12-31"}}
    EEATimeseriesReader(
        "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/renamed/",
        filters=filters,
    )
