def _py_to_pg(val, dtype):
    def _simple_py_to_pg(val):
        if type(val) == str:
            return f"'{val}'"
        if type(val) == int or type(val) == float:
            return "{val}"
        if type(val) == bool:
            return "{val}".lower()
    if type(val) == list:
        return f"ARRAY[{', '.join(map(_simple_py_to_pg, val))}]::{dtype}"
    else:
        return _simple_py_to_pg(val)
            
def create_table_query(table_name: str, columns: list, is_prod: bool = False, tail: str=""):
    tail = tail if is_prod else ""

    def _build_field(col, is_prod):
        if is_prod:
            return f"{col['name']} {col['type']} {col.get('mode', '')} {col.get('prod', '')}"
        else:
            return f"{col['name']} {col['type']}"

    def _add_column(col):
        return f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {_build_field(col, False)};\n"

    def _set_default_vals(col):
        default = _py_to_pg(col['default'], col['type'])
        return f"{col['name']} = coalesce({col['name']}, {default})"

    def _set_nullability(col):
        constraint = "SET NOT NULL" if "not null" in col['mode'].lower() else "DROP NOT NULL"
        return f"ALTER TABLE {table_name} ALTER COLUMN {col['name']} {constraint};\n"

    fields = ",\n".join(
        map(lambda x: _build_field(x, is_prod), columns)
    )

    add_fields = "".join(
        map(_add_column, columns)
    )

    default_vals = ", ".join(
        map(_set_default_vals,
            filter(lambda x: 'default' in x.keys(), columns)
            )
    )
    default_vals = default_vals if is_prod else ""

    set_default_stmt = f"""
    UPDATE {table_name}
        SET {default_vals};
    """ if len(default_vals) > 0 else ""

    nullability_stmt = "".join(
        map(_set_nullability, columns)
    ) if is_prod else ""

    query = f"""
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS {table_name} (
    {fields}
    )
    {tail};
    {add_fields};
    {set_default_stmt};
    {nullability_stmt};
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
        
