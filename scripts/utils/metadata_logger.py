import redshift_connector

def get_connection():

    conn = redshift_connector.connect(
        host="ris360-wg-dev.467866449044.us-east-1.redshift-serverless.amazonaws.com:5439/ris_360_analytics",
        database="ris_360_analytics",
        user="admin",
        password="ChangeMe123!"
    )

    return conn


def log_pipeline_start(run_id):

    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
    INSERT INTO ris360_metadata.pipeline_runs
    (run_id, pipeline_name, status, start_time)
    VALUES
    ('{run_id}', 'RIS360_PIPELINE', 'RUNNING', GETDATE())
    """

    cursor.execute(query)
    conn.commit()


def log_pipeline_end(run_id, status):

    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
    UPDATE ris360_metadata.pipeline_runs
    SET status='{status}',
        end_time=GETDATE()
    WHERE run_id='{run_id}'
    """

    cursor.execute(query)
    conn.commit()