import dask.dataframe as dd
from tqdm import tqdm

INPUT_DIR = "/media/philipbl/Vargas/processed_data/"
OUTPUT_DIR = "/media/philipbl/Vargas/grouped_data/"

df = dd.read_parquet(INPUT_DIR, engine="pyarrow")

unique_ap_names = df["wlan.vs.aruba.ap_name"].unique().compute()

for ap_name in tqdm(unique_ap_names, desc="Processing AP Groups"):
    pandas_df = df[df["wlan.vs.aruba.ap_name"] == ap_name].compute()

    output_path = f"{OUTPUT_DIR}{ap_name}.parquet"
    pandas_df.to_parquet(output_path, engine="pyarrow")
