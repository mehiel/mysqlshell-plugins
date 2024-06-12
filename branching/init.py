# init.py
# -------
import mysqlsh
from mysqlsh.plugin_manager import plugin, plugin_function

class SessionNotAvailableError(Exception):
    def __init__(self, message=""):
        self.message = message

def get_current_session(session=None):
    """
    Get the current session object.
    """
    if session is None:
        shell = mysqlsh.globals.shell
        session = shell.get_session()
        if session is None:
            raise SessionNotAvailableError("No session specified. Either pass a session object to this "
                "function or connect the shell to a database")
    return session

@plugin
class branching:
    """
    A branching plugin that adds branching functionality to the shell.
    """

@plugin_function("branching.listBranches")
def list_branches(schema_name, session=None):
    """
    List the branches of the provided database.

    Args:
        schema_name (string): The name of the database to list the branches from
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    try:
        session = get_current_session(session)
    except SessionNotAvailableError as err:
        print(err)
        return

    databases = session.run_sql(f"SHOW DATABASES").fetch_all()

    # Iterate through the databases, split on $$ and if the first part matches
    # the schema name, print the branch name
    for database in databases:
        database_name_parts = database[0].split('$$')
        database_name = database_name_parts[0]
        database_branch = database_name_parts[1] if len(database_name_parts) > 1 else "mainline"
        if database_name == schema_name:
            print(database_branch)

@plugin_function("branching.createBranch")
def create_branch(schema_name, branch_name, session=None):
    """
    Creates a new branch of the provided database.

    Args:
        schema_name (string): The name of the database to be branched
        branch_name (string): The name of the branch to be created
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    try:
        session = get_current_session(session)
    except SessionNotAvailableError as err:
        print(err)
        return

    # Create the new database name
    branched_schema_name = schema_name + '$$' + branch_name

    # Create the new database
    branch_exists = session.run_sql(f"SELECT COUNT(*) as db_exists FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{branched_schema_name}';").fetch_one_object().db_exists
    if branch_exists == 1: 
        print(f"Branch {branch_name} already exists")
        return

    # Create the new database
    session.run_sql(f"CREATE DATABASE `{branched_schema_name}`")

    # Get a list of tables in the original database
    tables = session.run_sql(f"SHOW TABLES IN `{schema_name}`").fetch_all()

    # Iterate through the tables and copy them to the new database
    for table in tables:
        # Get the table name
        table_name = table[0]
        table_columns = session.run_sql(f"SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ',') FROM (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' AND EXTRA NOT LIKE '%VIRTUAL GENERATED%') AS table_columns").fetch_one()[0]
        # Copy the table`
        session.run_sql(f"CREATE TABLE `{branched_schema_name}`.{table_name} LIKE `{schema_name}`.{table_name}")
        session.run_sql(f"INSERT INTO `{branched_schema_name}`.{table_name} ({table_columns}) SELECT {table_columns} FROM `{schema_name}`.{table_name}")

    # Get a list of views in the original database
    views = session.run_sql(f"SHOW FULL TABLES IN `{schema_name}` WHERE Table_Type='View'").fetch_all()

    # Iterate through the views and copy them to the new database
    for view in views:
        # Get the view name
        view_name = view[0]
        session.run_sql(f"CREATE VIEW `{branched_schema_name}`.{view_name} AS SELECT * FROM `{schema_name}`.{view_name}")

@plugin_function("branching.deleteBranch")
def delete_branch(schema_name, branch_name, session=None):
    """
    Delete a branch of the provided database.

    Args:
        schema_name (string): The name of the database to delete the branch from
        branch_name (string): The name of the branch to be created
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    Returns:
        Nothing
    """
    try:
        session = get_current_session(session)
    except SessionNotAvailableError as err:
        print(err)
        return
    session.run_sql(f"DROP DATABASE IF EXISTS `{schema_name}$${branch_name}`")
