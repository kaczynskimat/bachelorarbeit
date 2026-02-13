import pandas as pd
import time
from baseline_experiment_zanon import BaselineExperiment
from data_generalization import GeneralizationExperiment
from temporal_aggregation import TemporalAggregationExperiment
from local_prefiltering_with_generalization import LocalPrefilteringWithGeneralization

def run_experiments():
    DATA_PATH = 'output.csv'
    MAX_Z = 50 # Max z to test
    
    # List to hold all results from all experiments
    all_experiments_data = []

    print(f"--- Starting Experiments (Max Z: {MAX_Z}) ---")

    # 1. --- BASELINE EXPERIMENT --- 
    print("Running: Baseline Experiment...")
    baseline = BaselineExperiment(DATA_PATH, max_z_to_test=MAX_Z)
    baseline.prepare_data()
    
    for res in baseline.results:
        res['experiment_type'] = 'Baseline'
        res['parameter'] = 'None'
        all_experiments_data.append(res)


    # 2. --- DATA GENERALIZATION (EDGE) --
    # 3 levels of precision: 0 (integer), 1, and 2 decimals
    precisions_to_test = [0, 1, 2] 
    
    for prec in precisions_to_test:
        print(f"Running: Generalization (Precision {prec})...")
        gen_exp = GeneralizationExperiment(DATA_PATH, precision=prec, max_z_to_test=MAX_Z)
        gen_exp.prepare_data()
        
        for res in gen_exp.results:
            res['experiment_type'] = 'Generalization'
            res['parameter'] = f"Precision {prec}"
            all_experiments_data.append(res)


    # 3. --- TEMPORAL AGGREGATION (EDGE) ---
    windows_to_test = ['1h', '2h', '4h'] # currently 3 window sizes
    
    for window in windows_to_test:
        print(f"Running: Temporal Aggregation (Window {window})...")
        agg_exp = TemporalAggregationExperiment(DATA_PATH, max_z_to_test=MAX_Z)
        agg_exp.prepare_data(window_size=window)
        
        for res in agg_exp.results:
            res['experiment_type'] = 'Temporal Aggregation'
            res['parameter'] = f"Window {window}"
            all_experiments_data.append(res)


    # 4. LOCAL PREFILTERING (FOG) WITH DATA GENERALIZATION (EDGE) 
    # different local_z thresholds at the Gateway
    local_z_values = [2, 5, 10]
    
    for lz in local_z_values:
        print(f"Running: Local Prefiltering (Local z={lz})...")
        # precision=2 is a default for this experiment to show combined effect; test for g_p = 1 and g_p = 0
        prefilter_exp = LocalPrefilteringWithGeneralization(
            DATA_PATH, 
            max_z_to_test=MAX_Z, 
            generalization_precision=2, 
            local_z=lz
        )
        prefilter_exp.round_energy_consumption()
        prefilter_exp.split_data_to_gateways()
        prefilter_exp.perform_local_prefiltering()
        prefilter_exp.global_z_anonymity()
        
        for res in prefilter_exp.results:
            res['experiment_type'] = 'Local Prefiltering'
            res['parameter'] = f"Local z={lz}"
            all_experiments_data.append(res)

    # --- SAVE RESULTS --- 
    print("--- Saving data... ---")
    final_df = pd.DataFrame(all_experiments_data)
    
    # Reorder columns for readability
    cols = [
        'experiment_type', 'parameter', 'z', 
        'published_tuples', 'suppressed_tuples', 
        'publication_ratio', 'ncp', 'bandwidth_savings',
        'publication_ratio_relative_to_gw'
    ]
    # Handle cases where keys might be missing (fills with NaN if missing)
    final_df = final_df.reindex(columns=cols)
    
    output_filename = '../results/thesis_results_final.csv'
    final_df.to_csv(output_filename, index=False)
    
    print(f"Success! Saved {len(final_df)} rows to {output_filename}")
    print(final_df.sample(5))

if __name__ == "__main__":
    start_time = time.time()
    run_experiments()
    print(f"Total execution time: {round(time.time() - start_time, 2)} seconds")