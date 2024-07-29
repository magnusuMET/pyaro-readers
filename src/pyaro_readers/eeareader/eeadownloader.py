import requests
import pprint
import polars as pl
import typer

# from pyarrow.dataset import dataset
import json
import pathlib
from pathlib import Path
import json
import csv
import toml
import os

import threading

from tqdm import tqdm


app = typer.Typer()


class EEADownloader:
    BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
    ENDPOINT = "ParquetFile/"
    URL_ENDPOINT = "urls"
    URL_POLLUTANT = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/"

    METADATFILE = "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/metadata.csv"
    DATAFILE = "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data.toml"

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

    def download_and_save(self, request: dict, save_loc: Path) -> None:
        urls = self._get_urls(request)

        if not save_loc.is_dir():
            save_loc.mkdir(parents=True, exist_ok=True)
        urls = urls.split("\r\n")[1:]
        if not isinstance(urls, list):
            urls = [urls]
        for url in urls:
            url = url.strip()
            if len(url) < 2:
                continue

            filename = url.split("/")[-1]
            # print(f"Downloading {filename}")
            try:
                result = requests.get(url)
            except Exception as e:
                raise ValueError(f"{url} failed to download due to {e}")
            with open(save_loc / filename, "wb") as f:

                f.write(result.content)

    def _make_request(self, request: dict):
        results = requests.post(self.BASE_URL + self.ENDPOINT, json=request)

        if results.status_code == 200:
            return results.content
        else:
            raise results.raise_for_status()

    def get_countries(self):
        country_file = requests.get(self.BASE_URL + "Country").json()
        return [country["countryCode"] for country in country_file]

    def get_station_metadata(self) -> dict:
        metadata = {}
        with open(self.METADATFILE, "r") as f:
            f.readline()
            for line in f:
                words = line.split(",")
                metadata[words[0]] = {
                    "lon": words[5],
                    "lat": words[6],
                    "alt": words[7],
                }

        return metadata

    def get_pollutants(self) -> dict:
        with open(self.DATAFILE, "r") as f:
            poll = toml.load(f)["pollutant"]
        return poll

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

    @app.command(name="download")
    def download_default(self, save_loc: Path, datasets: list[int] = [1]) -> None:

        if not save_loc.is_dir():
            save_loc.mkdir(parents=True, exist_ok=True)

        countries = self.get_countries()[:5]
        threads = []

        errorfile = open("errors.txt", "w")
        pbar = tqdm(countries, desc="Countries")
        for country in pbar:
            # print(f"Running for {country}")
            pbar.set_description(f"{country}")
            for poll in tqdm(self.DEFAULT_POLLUTANTS, desc="Pollutants", leave=False):
                full_loc = save_loc / poll / country

                request = {
                    "countries": [country],
                    "cities": [],
                    "properties": self.make_pollutant_url_list(poll),
                    "datasets": datasets,
                    "source": "Api",
                }
                self.download_and_save(request, full_loc)
                # try:
                #     self.download_and_save(request, full_loc)
                # except:
                #     errorfile.write(f"Failed for {country}, {poll}")
                #     continue

                # thread = threading.Thread(
                #     target=self.download_and_save,
                #     args=(
                #         request,
                #         full_loc,
                #     ),
                # )
        #         thread.start()
        #         threads.append(thread)

        # for thread in threads:
        #     thread.join()

        errorfile.close()

    def _postprocess_file(self, file: Path) -> pl.DataFrame:
        metadata = self.get_station_metadata()
        poll = self.get_pollutants()

        df = pl.read_parquet(file)
        pollutant = df.row(0)[1]
        station = df.row(0)[0].split("/")[-1]
        length = df.shape[0]
        for name in metadata[station]:
            series = pl.Series(name, [metadata[station][name]] * length)
            df.insert_column(1, series)

        series = pl.Series("PollutantName", [poll[str(pollutant)]] * length)
        df.insert_column(1, series)

        return df

    app.command(name="postprocess")

    def postprocess_all_files(self, from_folder: Path, to_folder: Path) -> None:

        polls = [str(x).split("/")[-1] for x in from_folder.iterdir() if x.is_dir()]
        if not to_folder.is_dir():
            to_folder.mkdir(parents=True, exist_ok=True)
        conversion_error = open(to_folder / "errors.txt", "w")
        error_n = 0
        for poll in tqdm(polls, desc="Pollutant"):
            countries = [
                str(x).split("/")[-1]
                for x in (from_folder / poll).iterdir()
                if x.is_dir()
            ]
            for country in tqdm(countries, desc="Country"):
                folder = from_folder / poll / country
                new_folder = to_folder / poll / country

                if not new_folder.is_dir():
                    new_folder.mkdir(parents=True, exist_ok=True)

                files = folder.glob("*.parquet")
                for file in tqdm(files, desc="Files"):
                    try:
                        df = self._postprocess_file(file)
                        df.write_parquet(new_folder / file.name)
                    except Exception as e:
                        # raise ValueError(f"{file} failed with {e}")
                        error_n += 1
                        conversion_error.write(
                            f"{error_n}: Error in converting {file} \n"
                        )
                        continue
        print(f"Finished with {error_n} errors")
        conversion_error.close()
        # new_filename = file.parent / f"processed_{file.name}"
        # df.write_parquet(new_filename)


@app.command(name="download")
def download(save_loc: Path):
    eead = EEADownloader()
    eead.download_default(save_loc)


@app.command(name="postprocess")
def postprocess(from_folder: Path, to_folder: Path):
    eead = EEADownloader()
    eead.postprocess_all_files(from_folder, to_folder)


if __name__ == "__main__":

    # app()

    eead = EEADownloader()
    eead.download_default(
        Path(
            "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data"
        )
    )
    # df = eead.open_files("NO_EEA/E1a/*.parquet")
    # breakpoint()
    # eead.postprocess_all_files(
    #     Path(
    #         "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/data"
    #     ),
    #     Path(
    #         "/home/danielh/Documents/pyaerocom/pyaro-readers/src/pyaro_readers/eeareader/renamed"
    #     ),
    # )
