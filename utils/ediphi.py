import os
from dotenv import load_dotenv
import requests
from json.decoder import JSONDecodeError
import json
import pandas as pd
from tenacity import retry, wait_fixed, wait_random, stop_after_attempt

load_dotenv()


def main():
    tenant = Database()
    line_items = tenant.get_table("line_items", df=True)
    print(line_items.head())


# -----------------------------------------------------------------------
# Database class


class Database:
    """
    Database instance for a tenant.

    Data structure also contains tables.

    Attributes
    ----------
    database_id : int
        uniquie identifier
    describe : dict
        api response containing high-level information about the database
    tanant_name : string
    tables : dict
        keys are table_name, values are table_id
    """

    def __init__(self):
        self.database_id = os.getenv("DATABASE_NO")
        self.describe = self._describe_db()
        self.tenant_name = self.describe["name"]
        self.tables = {i["name"]: i["id"] for i in self.describe["tables"]}

    def _describe_db(self):
        """
        Private method for Database to describe itself to itself
        """

        url = f'https://data.ediphi.com/api/database/{os.getenv("DATABASE_NO")}?include=tables'
        headers = {"X-API-KEY": os.getenv("X_API_KEY")}
        response = requests.request("GET", url, headers=headers)
        return json.loads(response.content)

    @retry(wait=wait_fixed(3) + wait_random(0, 2), stop=stop_after_attempt(5))
    def query(self, query: str, df: bool = False):
        """
        Method to execute sql on read-only database objects.

        Note
        ----------
        The database you are querying is a read-replica
        Query responses can fetch as many as 200_000 rows,
        unless query duration causes it to run past changes made on the primary.
        Therefore, setting limit to a lower value and iterating result sets is advised.
        Try fetching 1000 rows at a time

        Parameters
        ----------
        query : string
            must be valid sql
        df : bool, default: False
            Set to True to return results as pandas dataframe

        Returns
        -------
        dict | dataframe

        Examples
        --------
        Execute a query.

        >>> query = 'select name from estimates limit 1'
        >>> tenant = ediphi.Database()
        >>> res = tenant.query(query)
        >>> display(res)
        [{'name': 'Test Project AB'}]
        """

        url = "https://data.ediphi.com/api/dataset/json"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-API-KEY": os.getenv("X_API_KEY"),
        }
        data = {
            "query": json.dumps(
                {
                    "database": int(os.getenv("DATABASE_NO")),
                    "type": "native",
                    "native": {"query": f"{query}"},
                }
            )
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            result = json.loads(response.content)
            error = result["error"]
            raise ValueError(error)
        except TypeError:
            if df:
                return pd.DataFrame(result)
            else:
                return result
        except JSONDecodeError as j:
            raise ValueError(f"result size exceeds connection limit:\n  {j.msg}")

    def data_dictionary(self, table_name: str = None, df=False):
        """
        Method to fetch data dictionary for Database

            Uses the query method to execute the sql query at data_dictionary.sql

        Parameters
        ----------
        table_name : str, default: None
            Enter a table_name to retrieve data dictionary relative to that table only
        df : bool, default: False
            Set to True to return results as pandas dataframe

        Returns
        -------
        dict | dataframe

        Examples
        --------
        Retrive data dictionary relative to a table.

        >>> tenant = ediphi.Database()
        >>> df = tenant.data_dictionary('scope_alternate_bid', df=True)
        >>> display(df)
        +----+---------------------+-----------------+--------------------------+---------+---------+--------------------+---------------------+
        |    | table_name          | column_name     | data_type                | is_pk   | is_fk   | references_table   | references_column   |
        |----+---------------------+-----------------+--------------------------+---------+---------+--------------------+---------------------|
        |  0 | scope_alternate_bid | updated_at      | timestamp with time zone | False   | False   |                    |                     |
        |  1 | scope_alternate_bid | created_at      | timestamp with time zone | False   | False   |                    |                     |
        |  2 | scope_alternate_bid | id              | uuid                     | True    | False   |                    |                     |
        |  3 | scope_alternate_bid | type            | text                     | False   | False   |                    |                     |
        |  4 | scope_alternate_bid | value           | character varying(255)   | False   | False   |                    |                     |
        |  5 | scope_alternate_bid | bid             | uuid                     | False   | True    | bids               | id                  |
        |  6 | scope_alternate_bid | scope_alternate | uuid                     | False   | True    | scope_alternates   | id                  |
        +----+---------------------+-----------------+--------------------------+---------+---------+--------------------+---------------------+
        """

        with open("data_dictionary.sql", "r") as dd:
            query = dd.read()
        if table_name:
            query += f" where c.table_name = '{table_name}' or cf.references_table = '{table_name}'"
        return self.query(query, df)

    def get_table(
        self,
        table_name: str,
        limit: int = None,
        chunk_limit: int = 1000,
        pk: str = "id",
        properties={0: ""},
        df: bool = False,
    ):
        """
        Method to fetch the data from a table

            Uses the query method to execute a select statement on the target

        Parameters
        ----------
        table_name : str
            must exist in Database
        limit : int, default: None
            Set overall limit for result set if you like
        chunk_limit : int, default: 1000
            Controls chunk size. Lower values will result in more iterations with a lower failure rate
        pk : str, default: id
            Many tables have id as their primary key, but update this as needed for tables with other pk's
        properties : dict, default: {0:''}
            Put filter conditions to be used in the where clause here; e.g., {'id':1, 'foo':'bar'}
        df : bool, default: False
            Set to True to return results as pandas dataframe

        Returns
        -------
        dict | dataframe

        Examples
        --------
        Retrive data from a table.

        >>> tenant = ediphi.Database()
        >>> df = tenant.get_table('project_sort_fields', limit=5, properties={'project':'e794ad3a-f747-4409-a373-15be7b8f0be9'}, df=True)
        >>> df = df.iloc[:,:3]
        >>> display(df)
        +----+--------------------------------------+--------------------------------------+--------------------------------------+
        |    | id                                   | project                              | sort_field                           |
        |----+--------------------------------------+--------------------------------------+--------------------------------------|
        |  0 | 06790d74-72ab-4d23-8dd9-a25b551fa9aa | e794ad3a-f747-4409-a373-15be7b8f0be9 | 92d40bb6-52d3-4314-9158-e613eec6ff85 |
        |  1 | 4add8215-a7a6-46a8-8e59-964f8e68eb3e | ae1df2f3-119d-44e9-b33c-064a149a4ffb | 7e36c591-54f2-472d-9abc-f5c3d7855f89 |
        |  2 | 465c8055-53d2-4d57-bdc0-aa368f51658d | 91397197-74ff-47dd-8779-4af784eff99c | c3be304b-eadb-449b-9506-7203281fed0b |
        |  3 | 724d1f57-677d-4094-b0d5-0308d13c4f1a | ae1df2f3-119d-44e9-b33c-064a149a4ffb | 9adf10a2-9105-4ad2-8f03-3e3b580253c7 |
        |  4 | 8790b64b-efe7-42c2-92ca-3c1531169f5b | ae1df2f3-119d-44e9-b33c-064a149a4ffb | 499260d6-adfc-44af-921a-8b28590a90de |
        +----+--------------------------------------+--------------------------------------+--------------------------------------+
        """
        table_name = table_name.lower()
        if 0 not in properties.keys():
            properties = {0: "".join([f" and {k}={v}" for k, v in properties.items()])}
        if table_name in self.tables.keys():
            res, result, idx = (
                [
                    1,
                ],
                [
                    1,
                ],
                0,
            )
            if limit:
                if 0 < limit < chunk_limit:
                    init_query = f"select * from {table_name} where deleted_at is null {properties[0]} order by {pk} asc limit {limit}"
                    try:
                        res = self.query(init_query)
                        if df:
                            return pd.DataFrame(res)
                        else:
                            return res
                    except Exception as e:
                        return e
                else:
                    try:
                        init_query = f"select * from {table_name} where deleted_at is null {properties[0]} order by {pk} asc limit {chunk_limit}"
                        res = self.query(init_query)
                        collected_rows = chunk_limit
                        while (len(res) <= limit) & (len(result) > 0) & (idx < 100_000):
                            if chunk_limit + collected_rows > limit:
                                chunk_limit = limit - collected_rows
                            iter_query = f"select * from {table_name} where deleted_at is null and {pk} > '{res[-1][pk]}' {properties[0]} order by {pk} asc limit {chunk_limit}"
                            result = self.query(iter_query)
                            collected_rows += chunk_limit
                            res += result
                            idx += 1
                    except Exception as e:
                        raise ValueError(e)
            else:
                try:
                    init_query = f"select * from {table_name} where deleted_at is null {properties[0]} order by {pk} asc limit {chunk_limit}"
                    res = self.query(init_query)
                    while (len(result) > 0) & (idx < 100_000):
                        iter_query = f"select * from {table_name} where deleted_at is null and {pk} > '{res[-1][pk]}' {properties[0]} order by {pk} asc limit {chunk_limit}"
                        result = self.query(iter_query)
                        res += result
                        idx += 1
                except Exception as e:
                    raise ValueError(e)
            if df:
                return pd.DataFrame(res)
            else:
                return res
        else:
            raise ValueError(
                "The table_name you entered does not exist in the database"
            )


# -----------------------------------------------------------------------
# Table class


class Table(Database):
    """
    Table instance for a Database.

    Data structure also contains columns.

    Parameters
    ----------
    table_name : string

        Must exist in Database.

    Attributes
    ----------
    table_name : string
    table_id : int - uniquie identifier
    describe : dict - api response containing high-level information about the
    columns : dict - keys are column_id, values are {"name": column_name, "fk_target_field_id": fk_target_field_id}

    Examples
    --------
    Instatiating a table and inspecting its columns attribute.

    >>> cost_models = ediphi.Table('cost_models')
    >>> cost_models.columns
       {12698: {'name': 'id', 'fk_target_field_id': None},
        12697: {'name': 'name', 'fk_target_field_id': None},
        12694: {'name': 'project', 'fk_target_field_id': 12492},
        12699: {'name': 'owner', 'fk_target_field_id': 14683},
        12695: {'name': 'created_at', 'fk_target_field_id': None},
        12696: {'name': 'updated_at', 'fk_target_field_id': None}}
    """

    def __init__(self, table_name):
        super().__init__()
        self.table_name = table_name
        self.table_id = self.tables[table_name]
        self.describe = self._describe_table()
        self.columns = {
            i["id"]: {"name": i["name"], "fk_target_field_id": i["fk_target_field_id"]}
            for i in self.describe["fields"]
        }

    def _describe_table(self):
        """
        Private method for Table to describe itself to itself
        """

        url = f"https://data.ediphi.com/api/table/{self.table_id}/query_metadata"
        headers = {"X-API-KEY": os.getenv("X_API_KEY")}
        response = requests.request("GET", url, headers=headers)
        return json.loads(response.content)


if __name__ == "__main__":
    main()
