BQ_TO_PG = {
        "REQUIRED": "NOT NULL",
        "INTEGER": "bigint",
        "DATE": "DATE",
        "STRING": "TEXT",
        "NULLABLE": "",
        "FLOAT64": "double precision"
}
PG_TO_BQ = { v: k for k, v in BQ_TO_PG.items() }
DENOMER = "_staging"

def get_bq_schema(pg_schema):
    schema = []
    for col in pg_schema:
        bq_col = {}
        bq_col["name"] = PG_TO_BQ.get(col["name"])
        bq_col["type"] = PG_TO_BQ.get(col["type"])
        schema.append(bq_col)
    return schema
