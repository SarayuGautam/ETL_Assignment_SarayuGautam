from setup_connection import bhatbhateniWHConnection, bhatbhateniWHCursor


def get_ordered_columns(table_name):
    splitted_data = table_name.split(".")
    query = """select listagg(COLUMN_NAME,',') within group(order by ORDINAL_POSITION ) from "{}"."INFORMATION_SCHEMA"."COLUMNS" WHERE  TABLE_SCHEMA='{}' and upper(TABLE_NAME)=upper('{}') ;""".format(
        splitted_data[0], splitted_data[1], splitted_data[2])
    bhatbhateniWHCursor.execute(query)
    return bhatbhateniWHCursor.fetchone()[0]


def main():
    source_and_staging_tables = {
        'BHATBHATENI.TRANSACTIONS.COUNTRY': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CNTRY_T',
        'BHATBHATENI.TRANSACTIONS.REGION': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_RGN_T',
        'BHATBHATENI.TRANSACTIONS.STORE': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_LOCN_T',
        'BHATBHATENI.TRANSACTIONS.CATEGORY': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CTGRY_T',
        'BHATBHATENI.TRANSACTIONS.SUBCATEGORY': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_SUB_CTGRY_T',
        'BHATBHATENI.TRANSACTIONS.PRODUCT': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_PDT_T',
        'BHATBHATENI.TRANSACTIONS.CUSTOMER': 'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CUSTOMER_T',
        'BHATBHATENI.TRANSACTIONS.SALES': 'BHATBHATENI_WH.DW_STG.STG_F_BHATBHATENI_SLS_T', }

    for source_table, staging_table in source_and_staging_tables.items():
        source_columns = get_ordered_columns(source_table).split(',')
        staging_columns = get_ordered_columns(staging_table).split(',')
        source_column_list = ",".join(
            [f"source.{col}" for col in source_columns])

        merge_query = '''
    MERGE INTO {} AS Staging
    USING {} AS Source
    ON ({})
    WHEN NOT MATCHED THEN
        INSERT ({})
        VALUES ({})
    '''.format(staging_table, source_table, ' AND '.join([f'source.{col} = staging.{col}' for col in source_columns]),
               ', '.join(staging_columns), source_column_list)
        bhatbhateniWHCursor.execute(merge_query)
        bhatbhateniWHConnection.commit()

        # staging_and_temp_tables = {
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CNTRY_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_CNTRY_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_RGN_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_RGN_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_LOCN_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_LOCN_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CTGRY_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_CTGRY_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_SUB_CTGRY_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_SUB_CTGRY_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_PDT_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_PDT_T',
        #     'BHATBHATENI_WH.DW_STG.STG_D_BHATBHATENI_CUSTOMER_T': 'BHATBHATENI_WH.DW_TMP.TMP_D_BHATBHATENI_CUSTOMER_T',
        #     'BHATBHATENI_WH.DW_STG.STG_F_BHATBHATENI_SLS_T': 'BHATBHATENI_WH.DW_TMP.TMP_F_BHATBHATENI_SLS_T'}


main()
