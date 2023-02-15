from setup_connection import bhatbhateniWHConnection, bhatbhateniWHCursor


def get_ordered_columns(table_name, schema_name):
    schema_name, _, table_name = table_name.split('.')
    query = f"""
        SELECT listagg(COLUMN_NAME, ',') WITHIN GROUP(ORDER BY ORDINAL_POSITION)
        FROM "{schema_name}"."INFORMATION_SCHEMA"."COLUMNS"
        WHERE TABLE_SCHEMA = '{schema_name}'
        AND UPPER(TABLE_NAME) = UPPER('{table_name}')
    """
    cs.execute(query)
    return cs.fetchone()[0].split(',')
