import pandas_gbq

from config import PROJECT_ID


def load_table(dataset_name, table_name):
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{dataset_name}.{table_name}`
    """
    print("Loading table...")
    table = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    return table