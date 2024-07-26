import csv

metadata = []
with open("DataExtract.csv") as f:
    dataextract_full = csv.reader(f)

    for dataextract in dataextract_full:
        country = dataextract[0]
        station_code = dataextract[6]
        station_natcode = dataextract[7]
        station_name = dataextract[8]
        long = dataextract[11]
        lat = dataextract[12]
        alt = dataextract[13]
        station_area = dataextract[15]
        station_type = dataextract[16]

        samplingpoint_id = dataextract[9]
        # country_code = samplingpoint_id.split("_")[0]
        # samplingpoint_id = samplingpoint_id.split("_")[-1]

        metadata.append(
            f"{samplingpoint_id}, {country}, {station_code}, {station_natcode}, {station_name}, {long}, {lat}, {alt}, {station_area}, {station_type}, \n"
        )

with open("metadata.csv", "w") as f:
    for line in metadata:
        f.write(line)
