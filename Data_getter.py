import numpy as np
import pandas as pd
import requests
import re
import pathlib

class csv_getter:
    def __init__(self, year, variables, census_api_key=None):
        # check that year is valid
        self.year = year
        self.variables = variables

        if census_api_key:
            self.api_key = census_api_key
        else:
            raise ValueError("Census API key must be provided.")

        self.PARENT_DIRECTORY = pathlib.Path().resolve()
        self.data_path = self.PARENT_DIRECTORY / f"{self.year}"

        # create directory, if it doesn't exist.
        if not self.data_path.exists():
            self.data_path.mkdir()

    
    def check_for_data(self, year, variable):
        file_path = self.data_path / f"{variable}_{year}.csv"
        return file_path.exists()

    def Population(self):
        year = self.year

        if self.check_for_data(year, "Population"):
            return 

        url = f"https://api.census.gov/data/{year}/acs/acs5"
        params = {
            "get": "NAME,B01003_001E",
            "for": "county:*",
            "key": self.api_key
        }
        r = requests.get(url, params=params)
        r.raise_for_status()

        data = r.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df.rename(columns={
            "B01003_001E": "population"
        }, inplace=True)

        df["population"] = df["population"].astype(int)
        df["year"] = year

        return df
    
    def StormDamage(self):

        year = self.year

        if self.check_for_data(year, "StormDamage"):
            return

        # 1. Get the URL for the StormEvents details file for the given year
        base_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
        r = requests.get(base_url)
        r.raise_for_status()
        
        pattern = re.compile(
            fr'StormEvents_details-ftp_v1\.0_d{year}_c\d+\.csv\.gz'
        )
        match = pattern.search(r.text)
        if match:
            url = str(base_url + match.group(0))
        else:
            raise ValueError(f"No StormEvents details file found for year {year}")
        
        # 2. Download and read the CSV file into a DataFrame
        df = pd.read_csv(url, compression='gzip', low_memory=False)

        return df

    def MedianIncome(self):
        raise NotImplementedError("Method not implemented yet.")
    def MedianHouseAge(self):
        raise NotImplementedError("Method not implemented yet.")
    def ShorelineCounties(self):
        raise NotImplementedError("Method not implemented yet.")
    def watershedCounties(self):
        raise NotImplementedError("Method not implemented yet.")
    def totalhouses(self):
        raise NotImplementedError("Method not implemented yet.")

def data_getter(year, census_api_key, variables=None):

    # NOTE: validate variables

    getter = csv_getter(year, variables, census_api_key)

    # Retrieve data for each variable
    data_frames = {}
    for var in variables:
        method = getattr(getter, var)
        data_frames[var] = method()

    # Save DataFrames to CSV files
    for var, df in data_frames.items():
        if df is not None:
            df.to_csv(getter.data_path / f"{var}_{year}.csv", index=False)