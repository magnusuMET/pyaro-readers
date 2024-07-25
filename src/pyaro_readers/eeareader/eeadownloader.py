import requests
import pprint
import polars as pl
from pyarrow.dataset import dataset

class EEADownloader:
    BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
    ENDPOINT = "ParquetFile"

    request_body = dict(
        contries=[],
        cities=[],
        properties=[],
        datasets=[],
        source=""
    )

    def __init__(self, ) -> None:
        pass

    
    def _make_request(self, request: dict):
        results = requests.post(self.BASE_URL + self.ENDPOINT, json=request)

        if results.status_code == 200:
            return results.content
        else:
            raise results.raise_for_status()

    def get_countries(self):
        country_file = requests.get(self.BASE_URL + "Country").json()
        pprint.pp(country_file)


    def open_files(self, folder):
        #ds = dataset(folder + "/*.parquet", format="parquet")
        df = pl.scan_parquet(folder)
        return df




if __name__ == "__main__":
    eead = EEADownloader()
    request = {
  "countries": [
    "NO"
  ],
  "cities": [
    
  ],
  "properties": [
    
  ],
  "datasets": [
    1,2
  ],
  "source": "Api"
}
    #result = eead._make_request(request)
    #with open("NO_EEA.zip", "wb") as f:
    #    f.write(result)

    df = eead.open_files("NO_EEA/E1a/*.parquet")
    breakpoint()
