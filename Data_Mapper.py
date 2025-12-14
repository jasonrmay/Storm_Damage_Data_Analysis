class Mapper:
    """
    Given a csv file, quantify how normally distributed each variable is.
    Then determine what data transformation can be applied to all variables
    to make all the variables as normally distributed as possible.
    """
    def __init__(self, csv_file):
        self.csv_file = csv_file
        # NOTE: Check input file exists and is readable
        pass