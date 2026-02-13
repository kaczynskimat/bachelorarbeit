import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt

class TemporalAggregationExperiment:

    def __init__(self, data, max_z_to_test=500):
        self.df = pd.read_csv(data, index_col=False)
        if 'stdorToU' in self.df.columns:
            self.df.drop(columns=['stdorToU'], axis=1, inplace=True)
        self.all_frequencies = {}
        self.results = []
        self.z_values = range(1, max_z_to_test + 1)
        self.bandwidth_savings = 0 # 75 means 75% reduction in # of messages sent in comparison to baseline 
        self.window_size = "None"
    
    def _calculate_bandwidth_savings(self, aggregated_tuples, original_tuples):
        self.bandwidth_savings = 1 - (aggregated_tuples / original_tuples)
        self.bandwidth_savings *= 100 # Convert to percentage
        self.bandwidth_savings = round(self.bandwidth_savings, 2)


    def _aggregate_the_data(self, time_window='2h'):
        
        original_tuples = len(self.df)
        self.df['DateTime'] = pd.to_datetime(self.df['DateTime'])
        self.df = self.df.set_index('DateTime')
        self.df = (self.df.groupby('LCLid') # consider data in lclid bins
                        ['KWH/hh (per half hour) '] # work only on this column
                        .resample(time_window) # consider data only in the time_window - it works on time data
                        .mean() # replace missing data with mean
                        .reset_index() # reset the index
                        .round(3)) # round to 3 decimal places
        self.df.dropna(inplace=True)
        aggregated_tuples = len(self.df)

        self._calculate_bandwidth_savings(aggregated_tuples, original_tuples)
        

    def _group_by_datetime(self):
        # Group by timestamp
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
                'bandwidth_savings': self.bandwidth_savings,
                'precision': "Precision 3", # 3 is a constant here because of the rounding after aggregation 
                'window': self.window_size
            })


    def prepare_data(self, window_size='2h'):
        self.window_size = window_size
        self._aggregate_the_data(window_size) # default window size is 2h
        self._group_by_datetime()
        self.perform_z_anon()

