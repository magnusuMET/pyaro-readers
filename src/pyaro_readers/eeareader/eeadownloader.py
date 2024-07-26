import requests
import pprint
import polars as pl

# from pyarrow.dataset import dataset
import json
import pathlib
from pathlib import Path
import json
import csv
import toml

import threading


class EEADownloader:
    BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
    ENDPOINT = "ParquetFile/"
    URL_ENDPOINT = "urls"
    URL_POLLUTANT = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/"

    DEFAULT_POLLUTANTS = [
        "SO2",
        "PM10",
        "PM2.5",
        "O3",
        "NO2",
        "CO",
        "NO",
        "OC",
        "EC",
        "C6H6",
        "EC in PM2.5",
        "OC in PM2.5",
        "EC in PM10",
        "OC in PM10",
        "NH4+ in PM2.5",
        "NO3- in PM2.5",
        "NH4+ in PM10",
        "NO3- in PM10",
        "HNO3",
    ]

    request_body = dict(contries=[], cities=[], properties=[], datasets=[], source="")

    def __init__(
        self,
    ) -> None:
        pass

    def _get_urls(self, request: dict):
        results = requests.post(
            self.BASE_URL + self.ENDPOINT + self.URL_ENDPOINT, json=request
        )

        if results.status_code == 200:
            return results.text
        else:
            raise results.raise_for_status()

    def download_and_save(self, request: dict, save_loc: Path):
        urls = self._get_urls(request)

        if not save_loc.is_dir():
            save_loc.mkdir(parents=True, exist_ok=True)

        urls = urls.split("\r\n")[1:]
        # print(f"Starting with {request['countries']}")
        for url in urls:
            if len(url) < 2:
                continue
            filename = url.split("/")[-1]
            # print(f"Downloading {filename}")
            result = requests.get(url)
            with open(save_loc / filename, "wb") as f:
                f.write(result.content)
        # print(f"Done with {request['countries']}")

    # def make_request_dict(self, countries: list = [], cities: list = [], properties: list = [], datasets: list = [1], source)

    def _make_request(self, request: dict):
        results = requests.post(self.BASE_URL + self.ENDPOINT, json=request)

        if results.status_code == 200:
            return results.content
        else:
            raise results.raise_for_status()

    def get_countries(self):
        country_file = requests.get(self.BASE_URL + "Country").json()
        return [country["countryCode"] for country in country_file]

    def open_files(self, folder):
        # ds = dataset(folder + "/*.parquet", format="parquet")
        df = pl.scan_parquet(folder)
        return df

    def make_pollutant_url_list(self, pollutants: list[str]) -> list[str]:
        urls = []
        with open(
            "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data.toml"
        ) as f:
            poll = toml.load(f)["pollutant"]
            for key in poll:
                if poll[key] in pollutants:
                    urls.append(self.URL_POLLUTANT + key)

        return urls

    def download_default(self, save_loc: Path, datasets: list[int] = [1]):

        if not save_loc.is_dir():
            save_loc.mkdir(parents=True, exist_ok=True)

        countries = self.get_countries()
        threads = []
        for country in countries:
            # print(f"Running for {country}")
            for poll in self.DEFAULT_POLLUTANTS:
                full_loc = save_loc / poll / country

                request = {
                    "countries": [country],
                    "cities": [],
                    "properties": self.make_pollutant_url_list(poll),
                    "datasets": datasets,
                    "source": "Api",
                }

                thread = threading.Thread(
                    target=self.download_and_save,
                    args=(
                        request,
                        full_loc,
                    ),
                )
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    eead = EEADownloader()
    request = {
        "countries": ["NO"],
        "cities": [],
        "properties": eead.make_pollutant_url_list(eead.DEFAULT_POLLUTANTS),
        "datasets": [1],
        "source": "Api",
    }
    # result = eead._make_request(request)
    # with open("NO_EEA.zip", "wb") as f:
    #    f.write(result)
    # eead.download_and_save(request=request, save_loc=Path("./data/"))
    eead.download_default(Path("./data"))
    # df = eead.open_files("NO_EEA/E1a/*.parquet")
    # breakpoint()
