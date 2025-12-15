import numpy as np
import pandas as pd
import requests
import re
import pathlib

class csv_getter:
    """
    A class to get various datasets from online sources for a given year.

    Available methods:
    Population_var, MedianIncome_var, HouseAge_var, ShorelineCounties_var, WatershedCounties_var, StormDamage_var

    Parameters:
    year (int): The year for which data is to be retrieved.
    variables (list of str): List of variable names to retrieve. If None, all variables are retrieved.
    census_api_key (str): API key for accessing Census data.

    Returns:
    Downloads DataFrames for each requested variable into CSV files in the parent directory named after the year.
    """
    def __init__(self, year, variables = None, census_api_key=None):

        # check that year is valid (range 2010-2023 for this example)
        if not (2010 <= year <= 2023):
            raise ValueError("Year must be between 2010 and 2023.")
        self.year = year

        self.methods = [m for m in dir(self) if m.endswith("_var")]

        if variables is None:
            self.variables = self.methods
        else:
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
        file_path = self.data_path / f"{variable[:-4]}.csv"
        return file_path.exists()

    def Population_var(self):
        year = self.year

        # population year is from the year before the storm data to account for post-storm changes (deaths, migration, etc.)
        year -= 1

        if self.check_for_data(year, "Population_var"):
            return 

        url = f"https://api.census.gov/data/{year}/acs/acs5"
        params = {
            "get": "NAME,B01003_001E",
            "for": "county:*",
            "key": self.api_key
        }
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df.rename(columns={
            "B01003_001E": "population"
        }, inplace=True)

        df["population"] = df["population"].astype(int)

        return df
    
    def StormDamage_var(self):

        year = self.year

        if self.check_for_data(year, "StormDamage_var"):
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

    def MedianIncome_var(self):
        """
        Download ACS 5-year county-level median household income for a given year
        and save it as a CSV.

        Parameters
        ----------
        year : int
            ACS 5-year estimate year (must be between 2009 and 2023)
        api_key : str
            Census API key
        out_dir : str
            Directory where the CSV will be written

        Output
        ------
        Writes a CSV named:
        county_median_household_income_<year>.csv
        """

        year = self.year
        api_key = self.api_key

        if self.check_for_data(year, "MedianIncome_var"):
            return

        url = f"https://api.census.gov/data/{year}/acs/acs5"
        params = {
            "get": "NAME,B19013_001E",
            "for": "county:*",
            "in": "state:*",
            "key": api_key
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df.rename(columns={"B19013_001E": "median_household_income"}, inplace=True)

        return df
    
    def HouseAge_var(self):
        """
        Downloads ACS 5-year county-level home age data (Table B25034)
        for a given year and saves it as a CSV.

        Parameters
        ----------
        year : int
            ACS 5-year end year (e.g., 2019, 2020, 2021, 2022)
        api_key : str
            Census API key
        output_csv : str
            Path to output CSV file
        """

        year = self.year
        api_key = self.api_key

        if self.check_for_data(year, "MedianHouseAge_var"):
            return

        # ACS 5-year endpoint
        base_url = f"https://api.census.gov/data/{year}/acs/acs5"

        # B25034: Year structure built
        variables = [
            "NAME",
            "B25034_001E",  # Total housing units
            "B25034_002E",  # Built 2020 or later (recent years vary by ACS year)
            "B25034_003E",  # Built 2010 to 2019
            "B25034_004E",  # Built 2000 to 2009
            "B25034_005E",  # Built 1990 to 1999
            "B25034_006E",  # Built 1980 to 1989
            "B25034_007E",  # Built 1970 to 1979
            "B25034_008E",  # Built 1960 to 1969
            "B25034_009E",  # Built 1950 to 1959
            "B25034_010E"   # Built 1940 to 1949
        ]

        params = {
            "get": ",".join(variables),
            "for": "county:*",
            "in": "state:*",
            "key": api_key
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()

        df = pd.DataFrame(data[1:], columns=data[0])

       # Convert numeric columns
        for col in variables[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # rename columns for clarity
        df.rename(columns={
            "B25034_001E": "total_housing_units",
            "B25034_002E": "built_2020_or_later",
            "B25034_003E": "built_2010_to_2019",
            "B25034_004E": "built_2000_to_2009",
            "B25034_005E": "built_1990_to_1999",
            "B25034_006E": "built_1980_to_1989",
            "B25034_007E": "built_1970_to_1979",
            "B25034_008E": "built_1960_to_1969",
            "B25034_009E": "built_1950_to_1959",
            "B25034_010E": "built_1940_to_1949"
        }, inplace=True)

        return df

    def ShorelineCounties_var(self):
        raise NotImplementedError("Method not implemented yet.")
    def WatershedCounties_var(self):
        raise NotImplementedError("Method not implemented yet.")

def data_getter(year, census_api_key, variables=None):

    getter = csv_getter(year, variables, census_api_key)

    # validate variables
    methods = dir(getter)
    for var in variables:
        if var not in methods:
            raise ValueError(f"Variable '{var}' is not a valid method of csv_getter.")

    # Retrieve data for each variable
    data_frames = {}
    for var in variables:
        method = getattr(getter, var)
        data_frames[var] = method()

    # Save DataFrames to CSV files
    for var, df in data_frames.items():
        if df is not None:
            df.to_csv(getter.data_path / f"{var[: -4]}.csv", index=False)
            