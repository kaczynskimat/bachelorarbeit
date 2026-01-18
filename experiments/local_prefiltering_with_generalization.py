import pandas as pd 
import matplotlib.pyplot as plt
from collections import Counter, defaultdict

class LocalPrefilteringWithGeneralization:

    def __init__(self, data, max_z_to_test=500, generalization_precision=2, local_z = 2):
        """
        Initializes the data from a CSV file and creates a DataFrame.
        Data will be tested for z values from 1 to 500.
        Generalization precision sets the decimal number precision ->
        E.g., 0.329 becomes 0.33 with decimal_precision = 2. 
        """
        self.df = pd.read_csv(data, index_col=False)
        if 'stdorToU' in self.df.columns:
            self.df.drop(columns=['stdorToU'], axis=1, inplace=True)
        self.precision = generalization_precision
        self.all_frequencies = {}
        self.results = []
        self.z_values = range(1, max_z_to_test + 1)
        self.gws = [] # a list of GWs with corresponding data
        self.local_z = local_z # change that to the range of z's to test 
        self.global_freq_map = []
        self.global_counters = defaultdict(Counter)
    
    def round_energy_consumption(self):
        """A helper function to round the energy consumption in the dataset."""
        self.df['KWH/hh (per half hour) '] = self.df['KWH/hh (per half hour) '].apply(lambda x: round(x, self.precision))
        
    def head(self, n=5):
        return self.df.head(n)
    
    def _calculate_ncp(self):
        min_value = self.df['KWH/hh (per half hour) '].min()
        max_value = self.df['KWH/hh (per half hour) '].max()

        precision = 10**(-self.precision)/(max_value - min_value)
        precisionloss_in_procent = precision * 100
        return round(precisionloss_in_procent, 2)
    
    def split_data_to_gateways(self):
        """
        Returns a list of dataframes.
        dfs[n] is data that belongs to n-th gateway
        """
        lclids = sorted(self.df['LCLid'].unique())
        groups = [lclids[i:i+100] for i in range(0, len(lclids), 100)]
        print(len(groups))
        # it splits the data into gateway groups
        for n in range(len(groups)):
            # select rows for macs that in groups[n]
            mask = self.df['LCLid'].isin(groups[n]) # keep only rows that are in group [n], it keeps only rows 
            # from the 1st GW, 2nd GW, etc.
            self.gws.append(self.df[mask]) # add the filtered data to the list of GWs

    def _build_frequency_map_final(self):
        for timestamp, group in self.df.groupby('DateTime'):
            # For one timestamp, we assign the frequencies ('0.123': 75, '0.234': 20, etc.) calculated in an optimized way by the Counter (on the energy column)
            self.all_frequencies[timestamp] = Counter(group['KWH/hh (per half hour) '])

    def _build_frequency_map(self, df):
        freq_map = {}
        for timestamp, group in df.groupby('DateTime'):
            freq_map[timestamp] = Counter(group['KWH/hh (per half hour) '])
        
        return freq_map
    
    def perform_local_prefiltering(self):
        for df in self.gws:
            freq_map = self._build_frequency_map(df)
            new_freq_map = {
                timestamp: Counter({
                    key: max(0, count - (self.local_z - 1))
                    for key, count in inner_counter.items()
                })
                for timestamp, inner_counter in freq_map.items() # outer loop, dictionary comprehension
            }

            self.global_freq_map.append(new_freq_map)

        # print(self.global_freq_map)
        # global_view_of_counters = add_counters(global_freq_map)
        for df in self.global_freq_map:
            for timestamp, counter in df.items():
                self.global_counters[timestamp] += counter  

        # print(self.global_counters)      


    def global_z_anonymity(self):

        total_tuples = len(self.df)
        total_forwarded_by_gateway = sum(counter.total() for counter in self.global_counters.values())

        max_z_to_test = 100
        z_values = range(1, max_z_to_test + 1)
        
        for z in z_values:
            total_published_for_this_z = 0
            for timestamp in self.global_counters:
                for _, count in self.global_counters[timestamp].most_common():
                    if count >= z:
                        total_published_for_this_z += (count - (z-1))

            if total_forwarded_by_gateway > 0:
                publication_ratio_relative_to_gw = (total_published_for_this_z / total_forwarded_by_gateway) * 100
            else:
                publication_ratio_relative_to_gw = 0

            publication_ratio = (total_published_for_this_z / total_tuples) * 100

            self.results.append({'z': z, 'published_tuples': total_published_for_this_z, 'publication_ratio_relative_to_gw': publication_ratio_relative_to_gw,
                        'publication_ratio': publication_ratio})

        # print(self.results)

    def draw_graphs(self):
        results_df = pd.DataFrame(self.results)
        print("\nSample of results:")
        print(results_df.head())
        print("\nResult for z=10:")
        print(results_df[results_df['z'] == 10])


        plt.plot(results_df['z'], results_df['published_tuples'])
        plt.xlabel("z-anonymity threshold")
        plt.ylabel("Published Tuples")
        plt.title(f"Number of published tuples for local prefiltering experiment (at CE level) with local z = {self.local_z}")
        plt.grid(True)
        plt.show()

        plt.plot(results_df['z'], results_df['publication_ratio'])
        plt.xlabel("z-anonymity threshold")
        plt.ylabel("% of tuples published")
        plt.title("Publication Ratio")
        plt.grid(True)
        plt.show()

        plt.plot(results_df['z'], results_df['publication_ratio_relative_to_gw'])
        plt.xlabel("z-anonymity threshold")
        plt.ylabel("Published Tuples")
        plt.title("Publication Ratio in % (Relative to # of tuples released at GW)")
        plt.grid(True)
        plt.show()
            
        print(self.results)
