import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def csv_to_parquet(csv_file_path, parquet_file_path):

    df = pd.read_csv(csv_file_path)
    
    table = pa.Table.from_pandas(df)
  
    pq.write_table(table, parquet_file_path)
    
    print(f"CSV file '{csv_file_path}' converted to Parquet file '{parquet_file_path}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV file to Apache Parquet format")
    
    parser.add_argument("csv_file", help="Path to the input CSV file")
    parser.add_argument("parquet_file", help="Path to the output Parquet file")
    
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    parquet_file_path = args.parquet_file
    
    csv_to_parquet(csv_file_path, parquet_file_path)
