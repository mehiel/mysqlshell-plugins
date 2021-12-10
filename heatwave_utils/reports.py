from mysqlsh.plugin_manager import plugin, plugin_function

def __returnQueryStats(session):
   
    # Define the query to get the routines
    stmt = """SELECT query_id, left(query_text,40),
       JSON_EXTRACT(JSON_UNQUOTE(qkrn_text->'$**.sessionId'),'$[0]') AS session_id,
       JSON_EXTRACT(JSON_UNQUOTE(qkrn_text->'$**.totalBaseDataScanned'), '$[0]') AS data_scanned, 
       JSON_EXTRACT(JSON_UNQUOTE(qexec_text->'$**.error'),'$[0]') AS error_message 
       FROM performance_schema.rpd_query_stats
    """


    stmt = stmt + ";"
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    return result;

def __returnTraceInfo(session):
   
    # Define the query to get the routines
    stmt = "SELECT QUERY, TRACE->'$**.Rapid_Offload_Fails' FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE"


    stmt = stmt + ";"
    # Execute the query and check for warnings
    result = session.run_sql(stmt)
    return result;




@plugin_function("heatwave_utils.report_query_stats")
def report_query_stats(session=None):
    """
    Wizard to report query stats

    Args:
        session (object): The optional session object

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    result= __returnQueryStats(session )
    shell.dump_rows(result) 

    return ;

@plugin_function("heatwave_utils.report_trace_info")
def report_trace_info(session=None):
    """
    Wizard to report trace info

    Args:
        session (object): The optional session object

    """
    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    result= __returnTraceInfo(session )
    shell.dump_rows(result) 

    return ;
