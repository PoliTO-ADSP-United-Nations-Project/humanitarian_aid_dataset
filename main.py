import os.path

from dataset.VAL2GDataset import VAL2GDataset
from base.Pipeline import Pipeline
from step.val2g.Add2022MonthlyInflowStep import Add2022MonthlyInflow
from step.val2g.AddDistanceBetweenCountriesStep import AddDistanceBetweenCountriesStep
from step.val2g.AddPercentChangeStep import AddPercentChangeStep
from step.val2g.AddSumOfInflowAndRemoveNanStep import AddSumOfInflowAndRemoveNaN
from step.val2g.ChangeFormatStep import ChangeFormatStep
from step.val2g.MergeStep import MergeStep


def main():
    DATASET_FOLDER = os.path.join(os.getcwd(), 'build_dataset')
    if not os.path.exists(DATASET_FOLDER):
        os.makedirs(DATASET_FOLDER)

    # Build final dataset and its pipeline
    final_ds = VAL2GDataset(verbose=True)
    final_ds.set_pipeline(Pipeline(name='final_dataset_pipeline',
                                   pipeline=[
                                       Add2022MonthlyInflow(),
                                       AddSumOfInflowAndRemoveNaN(),
                                       AddDistanceBetweenCountriesStep(),
                                       AddPercentChangeStep(),
                                       ChangeFormatStep(),
                                       MergeStep()
                                   ]))
    final_ds.do_pipeline(download_first=True, path=DATASET_FOLDER)


if __name__ == '__main__':
    main()
