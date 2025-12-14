import numpy as np
import pandas as pd

class MultipleLinearRegression:
    def __init__(self, year):
        # check if year is valid
        self.year = year
    
    def load_variables(self):
        """
        Find folder labeled by year and load the data from there. 
        The data is expected to be in a CSV file named 'VARIABLE.csv'.
        """
        # check if folder exits
        # check if files exists