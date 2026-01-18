from baseline_experiment_zanon import BaselineExperiment
from local_prefiltering_with_generalization import LocalPrefilteringWithGeneralization
from data_generalization import GeneralizationExperiment
from temporal_aggregation import TemporalAggregationExperiment
def main():
    data = 'output.csv'

    # baseline experiment
    experiment = BaselineExperiment(data)
    experiment.prepare_data()
    experiment.show_graphs()

    # local prefiltering with generalization
    # experiment2 = LocalPrefilteringWithGeneralization(data)
    # experiment2.round_energy_consumption()
    # print("Energy consumption rounded")
    # experiment2.split_data_to_gateways()
    # experiment2.perform_local_prefiltering()
    # print("local prefiltering done")
    # experiment2.global_z_anonymity()
    # experiment2.draw_graphs()

    # generalization experiment
    # experiment3 = GeneralizationExperiment(data, precision=3)
    # experiment3.prepare_data()
    # results = experiment3.get_results()
    # print(results)
    # experiment3.draw_graphs()
    # prec_los = experiment3.calculate_ncp()
    # print(prec_los)
    # experiment4 = GeneralizationExperiment(data, precision=2)
    # experiment4.prepare_data()
    # experiment4.draw_graphs()
    # prec_los = experiment4.calculate_ncp()
    # print(prec_los)
    # experiment5 = GeneralizationExperiment(data, precision=1)
    # experiment5.prepare_data()
    # experiment5.draw_graphs()
    # prec_los = experiment5.calculate_ncp()
    # print(prec_los)

    # temporal aggregation experiment
    # experiment6 = TemporalAggregationExperiment(data)
    # experiment6.prepare_data()
    # experiment6.draw_graphs()

    

    


if __name__ == "__main__":
    main()
