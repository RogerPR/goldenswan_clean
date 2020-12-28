import os
os.chdir("C:/Users/Roger/Desktop/projects/")

from kedro.extras.datasets.pandas.csv_dataset import CSVDataSet
from kedro.extras.datasets.pickle.pickle_dataset import PickleDataSet

csv_loader = CSVDataSet("03_primary/train_df.csv")
df = csv_loader.load()

pickle_loader = PickleDataSet("07_model_output/signal_store_inf.pkl")
signal_store_inf = pickle_loader.load()

#############
def aa(tt):
