from src.defs.postgre import utils

def create_table_query(table_name: str, columns: list, is_prod: bool = False,
                       tail: str="", drop: bool=False):
    query = "BEGIN TRANSACTION;\n"
    if drop == True:
        query += f"DROP TABLE IF EXISTS {table_name};\n"
    query += f"CREATE TABLE IF NOT EXISTS {table_name}(\n"
    
    for i, col in enumerate(columns):
        if type(col) == str:
            query += col
        else:
            query += f"\t{col['name']} {col['type']} {col['mode']}"
            if is_prod:
                query += " " + col.get("prod", "")
        if i != len(columns) - 1:
            query += ","
        query += "\n"

    tail = tail if is_prod else ""
    query += f") {tail};\n"
    query += "END TRANSACTION;"
    return query

def create_staging_table_query(table_name: str,
                               denomer=utils.DENOMER):
    staging_name = table_name+denomer
    query = f"""
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS {staging_name};
    CREATE TABLE {staging_name} ( LIKE {table_name} );
    END TRANSACTION;
    """
    return query

def staging_to_live_query(table_name: str,
                          staging_name: str, mode: str,
                          tail: str = "",
                          key: str = None,
                          columns: list = [],
                          drop_staging: bool = False):
    query = "BEGIN TRANSACTION;\n"
    if mode == "WRITE_TRUNCATE":
        query += write_truncate(table_name, staging_name, columns)
    if mode == "REPLACE_TABLE":
        query += replace_table(table_name, staging_name)
    if mode == "UPSERT":
        query += upsert(table_name, staging_name, key, columns)
    query += tail
    query += f"DROP TABLE IF EXISTS {staging_name};\n" if drop_staging else ""
    query += "END TRANSACTION;"
    return query

def write_truncate(table_name,
                   staging_name,
                   columns,
                   FILTER="",
                   transaction_block=False,
                   distinct_columns=[]):
    column_list = ", ".join(columns)
    distinct_filter = _build_distinct_filter(distinct_columns)
    query = ""
    query += "BEGIN TRANSACTION;\n" if transaction_block else ""
    query += f"DELETE FROM {table_name};\n"
    query += f"""
    INSERT INTO {table_name}({column_list})
    SELECT {distinct_filter} {column_list}
    FROM {staging_name}
    {FILTER};\n"""
    query += "END TRANSACTION;\n" if transaction_block else ""
    return query

def replace_table(table_name, staging_name):
    query = ""
    query += f"DROP TABLE IF EXISTS {table_name};\n"
    query += f"ALTER TABLE {staging_name} RENAME TO {table_name};\n"
    return query


def upsert(table_name, staging_name, key, columns):
    column_list = ", ".join(columns)
    upsert_columns = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns])
    query = f"""
    INSERT INTO {table_name}({column_list})
    SELECT {column_list} FROM {staging_name}
    ON CONFLICT ({key}) DO UPDATE SET {upsert_columns};
    """
    return query

def export_rows(table_name,
    export_table_name,
    columns,
    FILTER="",
    delete=False,
    clear_export_table=False):

    SQL = f"""
    BEGIN TRANSACTION;
    """
    SQL += f"DELETE FROM {export_table_name};" if clear_export_table else ""

    SQL += f"""
    INSERT INTO {export_table_name}
    SELECT {columns} FROM {table_name}
    {FILTER};
    """
    if delete:
        SQL += f"""
        DELETE FROM {table_name} 
        {FILTER};
        END TRANSACTION;
        """
    else:
        SQL +=" END TRANSACTION;"
    return SQL

def _build_distinct_filter(columns):
    if len(columns) == 0:
        return ""
    else:
        dcols = ", ".join(columns)
        return f"DISTINCT ON ({dcols})"
        
