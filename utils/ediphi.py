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

        with open("./queries/data_dictionary.sql", "r") as dd:
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
            res, result, idx = ("init", "init", 0)
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
# Estimate class


class Estimate(Database):
    """
    Estimate instance for a Database.

    Data structure also includes estimate lines, and uf and mf levels

    Parameters
    ----------
    estimate_id : string

        Must exist in Database.

    add_cols : list, default: []

        Additional columns to return from the line_items table.

    Attributes
    ----------
    estimate_id : string (uuid)
    add_cols : list, default: []
    estimate_name : string
    lines : dataframe of the base columns from the line_items table
    expanded_lines : dataframe from the line_items table incluing expanded sorts resulting from the expand_estimate_lines method
    uf_levels : list of integers - uniformat levels that exist within the estimate
    mf_levels : list of integers - masterformat levels that exist within the estimate

    Examples
    --------
    Instatiating an estimate and inspecting its uf_levels.

    >>> est = ediphi.Estimate(estimate_id='b5790ff4-1edb-49cc-a529-23d4401e24de')
    >>> display(est.uf_levels)
       {1, 2, 3, 4}
    """

    def __init__(self, estimate_id, add_cols=[]):
        super().__init__()
        self.estimate_id = estimate_id
        self.add_cols = add_cols
        self.estimate_name = self.query(
            f"select name from estimates where id = '{self.estimate_id}'"
        )[0]["name"]
        self.lines = self._get_lines()
        self.expanded_lines = None
        self.uf_levels = self._get_csi_levels("uf")
        self.mf_levels = self._get_csi_levels("mf")

    def _get_lines(self):
        """
        Private method for estimate to get its own lines
        """

        update = {
            "__ADD_COLS__": "\n".join(map(lambda x: f"    ,{x}", self.add_cols)),
            "__ESTIMATE_ID__": self.estimate_id,
        }
        with open("./queries/base_estimate_lines.sql", "r") as q:
            query = q.read()
            for i, j in update.items():
                query = query.replace(i, j)
        df = self.query(query=query, df=True)
        cols = [
            "id",
            "name",
            "quantity",
            "uom",
            "total_uc",
            "mf1_code",
            "mf2_code",
            "mf3_code",
            "uf1_code",
            "uf2_code",
            "uf3_code",
        ] + self.add_cols
        return df[cols]

    def _get_csi_levels(self, schema):
        """
        Private method for estimate to get its own csi levels
        """

        levels_query = (
            "select "
            + f"array(select distinct replace(jsonb_object_keys({schema}), '{schema}', '')::int "
            + "from line_items "
            + f"where estimate = '{self.estimate_id}')"
        )
        return eval(self.query(levels_query)[0]["array"])

    def describe_csi_sorts(
        self, df=None, schemas: list = ["mf", "uf"], levels: list = None
    ):
        """
        Method to add the descriptions for each csi code (masterformat and uniformat) to the lines dataframe

            Uses an internal function to build a sql query, which is then executed using the query method

        Parameters
        ----------
        df : dataframe, default: None
        schemas : list of strings, default: ['mf','uf']
            Must be either 'mf', 'uf', or both
        levels : list of integers, default: None
            Specify csi sort levels to return if you like. Otherwise, all available levels are returned

        Returns
        -------
        dataframe

        Examples
        --------
        Retrive descriptions for uniformat level 3.

        >>> est = ediphi.Estimate(estimate_id='b5790ff4-1edb-49cc-a529-23d4401e24de')
        >>> df = est.describe_csi_sorts(schemas=['uf',], levels=[3,])
        >>> df = df[['name', 'quantity', 'uom', 'uf3_code', 'uf3_desc']]
        >>> display(df)
        +----+-------------------------------------------------+------------+-------+------------+--------------------+
        |    | name                                            |   quantity | uom   | uf3_code   | uf3_desc           |
        |----+-------------------------------------------------+------------+-------+------------+--------------------|
        |  0 | Concrete Misc Wall - 10" Two-Sided              |          1 | sf    | B1010      | Floor Construction |
        |  1 | Concrete Parapet Wall - 8"                      |          1 | sf    | B2010      | Exterior Walls     |
        |  2 | Concrete Site Retaining Wall - 8"               |          1 | sf    | G2060      | Site Development   |
        |  3 | Wood Wall Paneling on Cleats - Premium Material |          1 | sf    | C2010      | Wall Finishes      |
        |  4 | Stainless Stl Door FOB                          |          1 | ea    | C1030      | Interior Doors     |
        +----+-------------------------------------------------+------------+-------+------------+--------------------+
        """

        def unpack_csi_json(schema, levels):
            root = "jsonb_array_elements(value)"
            elems = []
            for level in levels:
                if level == 1:
                    elem = root
                else:
                    elem = (
                        "jsonb_array_elements(" * (level - 1)
                        + root
                        + " ->'children')" * (level - 1)
                    )
                elems.append(f"{elem} ->> 'code' {schema}{level}_code")
                elems.append(f"{elem} ->> 'description' {schema}{level}_desc")
            return (
                "select\n   "
                + "".join(map(lambda x: f"{x}\n  ,", elems[:-1]))
                + elems[-1]
                + f"\nfrom setup s\nwhere key = 'sort_codes:{schema}'"
            )

        if any([i not in ["mf", "uf"] for i in schemas]) or (type(schemas) != list):
            raise ValueError(
                "Schema must be type list, and may contain mf, uf, or both"
            )
        if all([(type(levels) != list), levels is not None]):
            raise ValueError("Levels must be type list (or None to use all levels)")
        df = self.lines if df is None else df
        cols = list(df.columns)
        for schema in schemas:
            if levels is None:
                levels = self.mf_levels if schema == "mf" else self.uf_levels
            query = unpack_csi_json(schema, levels)
            df_csi = self.query(query=query, df=True)
            for n in levels:
                df_csi_l = df_csi[
                    [f"{schema}{n}_code", f"{schema}{n}_desc"]
                ].drop_duplicates()
                df = df.merge(df_csi_l, on=f"{schema}{n}_code", how="left")
                idx = cols.index(f"{schema}{n}_code")
                cols.insert(idx + 1, f"{schema}{n}_desc")
        return df[cols]

    def get_custom_sorts(self, df=None, sorts=None):
        """
        Method to add the codes and descriptions for each custom sort to the lines dataframe

            Uses the query method to execute a select statement on the sort_fields and sort_codes tables, as well as the extras column of the line_items table

        Parameters
        ----------
        df : dataframe, default: None
        sorts : list of strings, default: None
            Specify the names of custom sorts to return if you like. Otherwise, all available sorts are returned

        Returns
        -------
        dataframe

        Examples
        --------
        Retrive codes and descriptions for Bid Package.

        >>> est = ediphi.Estimate(estimate_id='b5790ff4-1edb-49cc-a529-23d4401e24de')
        >>> df = est.get_custom_sorts(sorts=['Bid Package',])
        >>> df = df[['name', 'quantity', 'uom', 'Bid Package_code', 'Bid Package_desc']]
        >>> display(df)
        +----+---------------------------------------------------+------------+-------+--------------------+--------------------------------------+
        |    | name                                              |   quantity | uom   |   Bid Package_code | Bid Package_desc                     |
        |----+---------------------------------------------------+------------+-------+--------------------+--------------------------------------|
        |  0 | Concrete Misc Wall - 10" Two-Sided                |          1 | sf    |               3.3  | Cast-In-Place Concrete               |
        |  1 | Subcontract - Scaffolding                         |          1 | ls    |              98.15 | Scaffolding                          |
        |  2 | Subcontract - Construction Surveying              |          1 | ls    |              98.3  | Construction Surveying               |
        |  3 | Subcontract - Dampproofing & Waterproofing        |          1 | ls    |               7.1  | Dampproofing & Waterproofing         |
        |  4 | Subcontract - Exterior Insulation & Finish System |          1 | ls    |               7.24 | Exterior Insulation & Finish Systems |
        +----+---------------------------------------------------+------------+-------+--------------------+--------------------------------------+
        """
        with open("./queries/sorts_estimate.sql", "r") as q:
            df_cs = self.query(
                query=q.read().replace("__ESTIMATE_ID__", str(self.estimate_id)),
                df=True,
            )
        sorts = sorts if sorts else df_cs["code_name"].drop_duplicates().to_list()
        df = self.lines if df is None else df
        cols = list(df.columns)
        for sort in sorts:
            df_cs_l = df_cs.loc[
                df_cs["code_name"] == sort, ["id", "code", "description"]
            ].drop_duplicates()
            df_cs_l.columns = ["id", f"{sort}_code", f"{sort}_desc"]
            df = df.merge(df_cs_l, on="id", how="left")
            cols += [f"{sort}_code", f"{sort}_desc"]
        return df[cols]

    def expand_estimate_lines(
        self, schemas: list = ["mf", "uf"], levels: list = None, sorts=None
    ):
        """
        Method to add the descriptions for each csi code (masterformat and uniformat), as well as the codes and descriptions for each custom sort to the lines dataframe

            Uses the describe_csi_sorts and get_custom_sorts methods

        Parameters
        ----------
        schemas : list of strings, default: ['mf','uf']
            Must be either 'mf', 'uf', or both
        levels : list of integers, default: None
            Specify csi sort levels to return if you like. Otherwise, all available levels are returned
        sorts : list of strings, default: None
            Specify the names of custom sorts to return if you like. Otherwise, all available sorts are returned

        Returns
        -------
        dataframe, stored in attribute: expanded_lines

        Examples
        --------
        Retrive descriptions for uniformat level 3, and codes and descriptions for Bid Package.

        >>> est = ediphi.Estimate(estimate_id='b5790ff4-1edb-49cc-a529-23d4401e24de')
        >>> df = est.expand_estimate_lines(schemas=['uf',], levels=[3,], sorts=['Bid Package',])
        >>> df = df[['name', 'uf3_code', 'uf3_desc', 'Bid Package_code', 'Bid Package_desc']]
        >>> display(df)
        +----+-------------------------------------------------+------------+----------------------+--------------------+----------------------------------+
        |    | name                                            | uf3_code   | uf3_desc             |   Bid Package_code | Bid Package_desc                 |
        |----+-------------------------------------------------+------------+----------------------+--------------------+----------------------------------|
        |  0 | Concrete Misc Wall - 10" Two-Sided              | B1010      | Floor Construction   |               3.3  | Cast-In-Place Concrete           |
        |  1 | Wood Wall Paneling on Cleats - Premium Material | C2010      | Wall Finishes        |               9.2  | Drywall                          |
        |  2 | Stainless Stl Door FOB                          | C1030      | Interior Doors       |               8.1  | Doors Frames & Hardware (Supply) |
        |  3 | Subcontract - Structural Demolition             | F3010      | Structure Demolition |               2.4  | Structural Demolition            |
        |  4 | Subcontract - Selective Demolition              | F3030      | Selective Demolition |               2.45 | Selective Demolition             |
        +----+-------------------------------------------------+------------+----------------------+--------------------+----------------------------------+
        """
        df = self.describe_csi_sorts(schemas=schemas, levels=levels)
        self.expanded_lines = self.get_custom_sorts(df=df, sorts=sorts)
        return self.expanded_lines


# -----------------------------------------------------------------------
# UPC class


class UPC(Database):
    """
    UPC instance for a Database.

    Data structure also includes upc lines, and uf and mf levels

    Parameters
    ----------
    add_cols : list, default: []

        Additional columns to return from the line_items table.

    Attributes
    ----------
    add_cols : list, default: []
    lines : dataframe of the base columns from the line_items table
    expanded_lines : dataframe from the line_items table incluing expanded sorts resulting from the expand_estimate_lines method
    uf_levels : list of integers - uniformat levels that exist within the estimate
    mf_levels : list of integers - masterformat levels that exist within the estimate

    Examples
    --------
    Instatiating a upc and inspecting its mf_levels.

    >>> upc = ediphi.UPC()
    >>> display(est.mf_levels)
       {1, 2, 3}
    """

    def __init__(self, add_cols=[]):
        super().__init__()
        self.add_cols = add_cols
        self.lines = self._get_lines()
        self.expanded_lines = None
        self.uf_levels = self._get_csi_levels("uf")
        self.mf_levels = self._get_csi_levels("mf")

    def _get_lines(self):
        """
        Private method for upc to get its own lines
        """

        update = {"__ADD_COLS__": "\n".join(map(lambda x: f"    ,{x}", self.add_cols))}
        with open("./queries/base_upc.sql", "r") as q:
            query = q.read()
            for i, j in update.items():
                query = query.replace(i, j)
        df = self.query(query=query, df=True)
        cols = [
            "id",
            "name",
            "uom",
            "mf1_code",
            "mf2_code",
            "mf3_code",
            "uf1_code",
            "uf2_code",
            "uf3_code",
        ] + self.add_cols
        return df[cols]

    def _get_csi_levels(self, schema):
        """
        Private method for upc to get its own csi levels
        """

        levels_query = (
            "select "
            + f"array(select distinct replace(jsonb_object_keys({schema}), '{schema}', '')::int res "
            + "from products order by res)"
        )
        return eval(self.query(levels_query)[0]["array"])

    def describe_csi_sorts(
        self, df=None, schemas: list = ["mf", "uf"], levels: list = None
    ):
        """
        Method to add the descriptions for each csi code (masterformat and uniformat) to the lines dataframe

            Uses an internal function to build a sql query, which is then executed using the query method

        Parameters
        ----------
        df : dataframe, default: None
        schemas : list of strings, default: ['mf','uf']
            Must be either 'mf', 'uf', or both
        levels : list of integers, default: None
            Specify csi sort levels to return if you like. Otherwise, all available levels are returned

        Returns
        -------
        dataframe

        Examples
        --------
        Retrive descriptions for uniformat level 3.

        >>> upc = ediphi.UPC()
        >>> df = upc.describe_csi_sorts(schemas=['uf',], levels=[3,])
        >>> df = df[['name', 'uom', 'uf3_code', 'uf3_desc']]
        >>> display(df)
        +----+---------------------------------------+-------+------------+-----------------------------+
        |    | name                                  | uom   | uf3_code   | uf3_desc                    |
        |----+---------------------------------------+-------+------------+-----------------------------|
        |  0 | Project Executive (Precon)            | hr    | Z1020      | Jobsite Management          |
        |  1 | Concrete SOMD - 5" total depth (LWC)  | sf    | B1010      | Floor Construction          |
        |  2 | Oil Fired WH/Commercial 411gal/600MBH | ea    | D2010      | Domestic Water Distribution |
        |  3 | Scissor Lift - 25-26' Narrow          | mo    | Z1050      | Project Requirements        |
        |  4 | Glass Premium - Ceramic Frit (Ext)    | sf    | B2020      | Exterior Windows            |
        +----+---------------------------------------+-------+------------+-----------------------------+
        """

        def unpack_csi_json(schema, levels):
            root = "jsonb_array_elements(value)"
            elems = []
            for level in levels:
                if level == 1:
                    elem = root
                else:
                    elem = (
                        "jsonb_array_elements(" * (level - 1)
                        + root
                        + " ->'children')" * (level - 1)
                    )
                elems.append(f"{elem} ->> 'code' {schema}{level}_code")
                elems.append(f"{elem} ->> 'description' {schema}{level}_desc")
            return (
                "select\n   "
                + "".join(map(lambda x: f"{x}\n  ,", elems[:-1]))
                + elems[-1]
                + f"\nfrom setup s\nwhere key = 'sort_codes:{schema}'"
            )

        if any([i not in ["mf", "uf"] for i in schemas]) or (type(schemas) != list):
            raise ValueError(
                "Schema must be type list, and may contain mf, uf, or both"
            )
        if all([(type(levels) != list), levels is not None]):
            raise ValueError("Levels must be type list (or None to use all levels)")
        df = self.lines if df is None else df
        cols = list(df.columns)
        for schema in schemas:
            if levels is None:
                levels = self.mf_levels if schema == "mf" else self.uf_levels
            query = unpack_csi_json(schema, levels)
            df_csi = self.query(query=query, df=True)
            for n in levels:
                df_csi_l = df_csi[
                    [f"{schema}{n}_code", f"{schema}{n}_desc"]
                ].drop_duplicates()
                df = df.merge(df_csi_l, on=f"{schema}{n}_code", how="left")
                idx = cols.index(f"{schema}{n}_code")
                cols.insert(idx + 1, f"{schema}{n}_desc")
        return df[cols]

    def get_custom_sorts(self, df=None, sorts=None):
        """
        Method to add the codes and descriptions for each custom sort to the lines dataframe

            Uses the query method to execute a select statement on the sort_fields and sort_codes tables, as well as the extras column of the products table

        Parameters
        ----------
        df : dataframe, default: None
        sorts : list of strings, default: None
            Specify the names of custom sorts to return if you like. Otherwise, all available sorts are returned

        Returns
        -------
        dataframe

        Examples
        --------
        Retrive codes and descriptions for Bid Package.

        >>> upc = ediphi.UPC()
        >>> df = upc.get_custom_sorts(sorts=['Bid Package',])
        >>> df = df[['name', 'uom', 'Bid Package_code', 'Bid Package_desc']]
        >>> display(df)
        +----+---------------------------------------------------------+-------+--------------------+------------------------------------------+
        |    | name                                                    | uom   |   Bid Package_code | Bid Package_desc                         |
        |----+---------------------------------------------------------+-------+--------------------+------------------------------------------|
        |  0 | Subcontract - Construction Surveying                    | ls    |               98.3 | Construction Surveying                   |
        |  1 | Subcontract - General Trades                            | ls    |               98.2 | General Trades                           |
        |  2 | Subcontract - Commissioning, Validation & Qualification | ls    |               98.6 | Commissioning Validation & Qualification |
        |  3 | Subcontract - Construction Material Testing             | ls    |               98.5 | Construction Material Testing            |
        |  4 | Project Executive (Precon)                              | hr    |               99   | Preconstruction                          |
        +----+---------------------------------------------------------+-------+--------------------+------------------------------------------+
        """
        with open("./queries/sorts_upc.sql", "r") as q:
            df_cs = self.query(query=q.read(), df=True)
        sorts = sorts if sorts else df_cs["code_name"].drop_duplicates().to_list()
        df = self.lines if df is None else df
        cols = list(df.columns)
        for sort in sorts:
            df_cs_l = df_cs.loc[
                df_cs["code_name"] == sort, ["id", "code", "description"]
            ].drop_duplicates()
            df_cs_l.columns = ["id", f"{sort}_code", f"{sort}_desc"]
            df = df.merge(df_cs_l, on="id", how="left")
            cols += [f"{sort}_code", f"{sort}_desc"]
        return df[cols]

    def expand_upc_lines(
        self, schemas: list = ["mf", "uf"], levels: list = None, sorts=None
    ):
        """
        Method to add the descriptions for each csi code (masterformat and uniformat), as well as the codes and descriptions for each custom sort to the lines dataframe

            Uses the describe_csi_sorts and get_custom_sorts methods

        Parameters
        ----------
        schemas : list of strings, default: ['mf','uf']
            Must be either 'mf', 'uf', or both
        levels : list of integers, default: None
            Specify csi sort levels to return if you like. Otherwise, all available levels are returned
        sorts : list of strings, default: None
            Specify the names of custom sorts to return if you like. Otherwise, all available sorts are returned

        Returns
        -------
        dataframe, stored in attribute: expanded_lines

        Examples
        --------
        Retrive descriptions for uniformat level 3, and codes and descriptions for Bid Package.

        >>> upc = ediphi.UPC()
        >>> df = upc.expand_upc_lines(schemas=['uf',], levels=[3,], sorts=['Bid Package',])
        >>> df = df[['name', 'uf3_code', 'uf3_desc', 'Bid Package_code', 'Bid Package_desc']]
        >>> display(df)
        +----+---------------------------------------+------------+-----------------------------+--------------------+------------------------+
        |    | name                                  | uf3_code   | uf3_desc                    |   Bid Package_code | Bid Package_desc       |
        |----+---------------------------------------+------------+-----------------------------+--------------------+------------------------|
        |  0 | Project Executive (Precon)            | Z1020      | Jobsite Management          |               99   | Preconstruction        |
        |  1 | Concrete SOMD - 5" total depth (LWC)  | B1010      | Floor Construction          |                3.3 | Cast-In-Place Concrete |
        |  2 | Oil Fired WH/Commercial 411gal/600MBH | D2010      | Domestic Water Distribution |               22   | Plumbing               |
        |  3 | Scissor Lift - 25-26' Narrow          | Z1050      | Project Requirements        |               99.1 | Project Requirements   |
        |  4 | Glass Premium - Ceramic Frit (Ext)    | B2020      | Exterior Windows            |                8.4 | Glazing                |
        +----+---------------------------------------+------------+-----------------------------+--------------------+------------------------+
        """
        df = self.describe_csi_sorts(schemas=schemas, levels=levels)
        self.expanded_lines = self.get_custom_sorts(df=df, sorts=sorts)
        return self.expanded_lines


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
