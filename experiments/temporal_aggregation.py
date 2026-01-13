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

    def _aggregate_the_data(self, time_window='2H'):

        self.df['DateTime'] = pd.to_datetime(self.df['DateTime'])

        self.df = self.df.set_index('DateTime')
        
        self.df = (self.df.groupby('LCLid') # consider data in lclid bins
                        ['KWH/hh (per half hour) '] # work only on this column
                        .resample(time_window) # consider data only in the time_window - it works on time data
                        .mean() # replace missing data with mean
                        .reset_index() # reset the index
                        .round(3)) # round to 3 decimal places
        
        self.df.dropna(inplace=True)

        print(self.df.tail())

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
            
            if total_published_for_this_z == 0 and z > 50: # Optimization to stop early
                print(f"No tuples published for z >= {z}. Stopping evaluation.")
                break

            publication_ratio = (total_published_for_this_z / total_tuples) * 100
                
                
            self.results.append({'z': z, 'published_tuples': total_published_for_this_z, 'publication_ratio': publication_ratio})


    def prepare_data(self):
        self._aggregate_the_data()
        self._group_by_datetime()
        self.perform_z_anon()
    
    def draw_graphs(self):
        results_df = pd.DataFrame(self.results)
        print("\nSample of results:")
        print(results_df.head())
        print("\nResult for z=50:")
        print(results_df[results_df['z'] == 50])

        plt.plot(results_df['z'], results_df['published_tuples'])
        plt.xlabel("z-anonymity threshold")
        plt.ylabel("Published Tuples")
        plt.title("Number of published tuples for Temporal Aggregation Experiment")
        plt.grid(True)
        plt.show()

        plt.plot(results_df['z'], results_df['publication_ratio'])
        plt.xlabel("z-anonymity threshold")
        plt.ylabel("% of tuples published")
        plt.title("Publication Ratio for Temporal Aggregation Experiment")
        plt.grid(True)
        plt.show()

    def bandwith_savings(self):
        no_rows = len(self.df)

    
