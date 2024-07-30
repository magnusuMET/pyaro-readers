import csv

metadata = []
with open("DataExtract.csv") as f:
    dataextract_full = csv.reader(f)

    for dataextract in dataextract_full:
        country = dataextract[0].strip()
        station_code = dataextract[6].strip()
        station_natcode = dataextract[7].strip()
        station_name = dataextract[8].strip()
        long = dataextract[11].strip()
        lat = dataextract[12].strip()
        alt = dataextract[13].strip()
        station_area = dataextract[15].strip()
        station_type = dataextract[16].strip()

        samplingpoint_id = dataextract[9]
        # country_code = samplingpoint_id.split("_")[0]
        # samplingpoint_id = samplingpoint_id.split("_")[-1]

        metadata.append(
            # f"{samplingpoint_id}, {country}, {station_code}, {station_natcode}, {station_name}, {long}, {lat}, {alt}, {station_area}, {station_type}, \n"
            f"{samplingpoint_id}, {country}, {station_code}, {long}, {lat}, {alt}, {station_area}, {station_type}, \n"
        )

with open("../metadata.csv", "w") as f:
    for line in metadata:
        f.write(line)
