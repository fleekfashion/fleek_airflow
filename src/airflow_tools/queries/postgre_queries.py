from src.defs.postgre import utils

def create_table_query(table_name: str, columns: str,
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
        if i != len(columns) - 1:
            query += ","
        query += "\n"

    query += f") {tail};\n"
    query += "END TRANSACTION;"
    return query

def create_staging_table_query(table_name: str, 
        denomer=utils.DENOMER):
    staging_name = table_name+denomer
    query1 = "BEGIN TRANSACTION;\n"
    query1 += f"DROP TABLE IF EXISTS {staging_name};\n"
    query1 += f"CREATE TABLE {staging_name} ( LIKE {table_name} );\n"
    query1 += f"END TRANSACTION;"
    return query1

def staging_to_live_query(table_name: str,
                          staging_name: str, mode: str,
                          tail: str="",
                          key: str=None):
    query = "BEGIN TRANSACTION;\n"
    if mode == "WRITE_TRUNCATE":
        query += _write_truncate(table_name, staging_name)
    if mode == "OVERWRITE":
        query += _overwrite_query(table_name, staging_name)
    if mode == "UPDATE_APPEND":
        query += _update_append(table_name, staging_name, key)
    query += f"DROP TABLE IF EXISTS {staging_name};\n"
    query += tail
    query += "END TRANSACTION;"
    return query

def _write_truncate(table_name, staging_name):
    query = ""
    query += f"TRUNCATE {table_name};\n"
    query += f"INSERT INTO {table_name} SELECT * FROM {staging_name};\n"
    return query

def _overwrite_query(table_name, staging_name):
    query = ""
    query += f"DROP TABLE {table_name};\n"
    query += f"ALTER TABLE {staging_name} RENAME TO {table_name};\n"
    return query


def _update_append(table_name, staging_name, key):
    query = ""
    select_diff = f"""SELECT t.* FROM {staging_name} s\n
RIGHT JOIN {table_name} t
   ON s.{key} = t.{key} 
   WHERE s.{key} = NULL
    """
    query += f"INSERT INTO {staging_name} {select_diff};\n"
    query += _write_truncate(table_name, staging_name)
    return query
