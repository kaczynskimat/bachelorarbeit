import pandas as pd 
import matplotlib.pyplot as plt
from collections import Counter

class BaselineExperiment:

    def __init__(self, data, max_z_to_test=500):
        """Initializes the data from a CSV file and creates a DataFrame."""
        self.df = pd.read_csv(data, index_col=False)
        if 'stdorToU' in self.df.columns:
            self.df.drop(columns=['stdorToU'], axis=1, inplace=True)
        self.all_frequencies = {}
        self.results = []
        self.z_values = range(1, max_z_to_test + 1)
    
    def head(self, n=5):
        return self.df.head(n)
    
    def _group_by_datetime(self):
        # Group by timestamp - it's faster than iterating row by row
        for timestamp, group in self.df.groupby('DateTime'):
            # For one timestamp, we assign the frequencies ('0.123': 75, '0.234': 20, etc.) calculated in an optimized way by the Counter (on the energy column)
            self.all_frequencies[timestamp] = Counter(group['KWH/hh (per half hour) '])

    def perform_z_anon(self):
        total_tuples = len(self.df)
        for z in self.z_values:
            total_published_for_this_z = 0
            for timestamp in self.all_frequencies:
                # all_frequencies[timestamp] is a Counter object, e.g., {0.123: 75, 0.456: 20}
                for count in self.all_frequencies[timestamp].values():
                    if count >= z:
                        total_published_for_this_z += (count - (z-1))

            publication_ratio = (total_published_for_this_z / total_tuples) * 100
                
            suppressed_count = total_tuples - total_published_for_this_z
            self.results.append({
                'z': z, 
                'published_tuples': total_published_for_this_z, 'publication_ratio': publication_ratio,
                'suppressed_tuples': suppressed_count,
                'ncp': 0.0,
                'bandwidth_savings': 0.0,
                'precision': 'Raw Data',
                'window': None
            })


    def prepare_data(self):
        self._group_by_datetime()
        self.perform_z_anon()

    
