import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--sql", type=str, required=True)
parser.add_argument("--mode", type=str, required=False)
parser.add_argument("--output_table", type=str, required=False)
args = parser.parse_args()

SQL = args.sql
MODE = args.mode
OUTPUT_TABLE = args.output_table
print(SQL)

# Run SQL
df = spark.sql(SQL)

if MODE == "WRITE_APPEND":
  df.write.insertInto(OUTPUT_TABLE, overwrite=False)
elif MODE == "WRITE_TRUNCATE":
  df.write.insertInto(OUTPUT_TABLE, overwrite=True)
