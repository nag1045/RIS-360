import redshift_connector


def get_connection():

    conn = redshift_connector.connect(
        host="ris360-wg-dev.467866449044.us-east-1.redshift-serverless.amazonaws.com",
        port=5439,
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

    cursor.close()
    conn.close()


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

    cursor.close()
    conn.close()


# check if the file has been processed 
def is_file_processed(file_name):

    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
    SELECT COUNT(*)
    FROM ris360_metadata.file_audit_log
    WHERE file_name = '{file_name}'
    AND file_status = 'PROCESSED'
    """

    cursor.execute(query)

    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return result > 0

# log the file status 
def log_file_status(file_name, dataset_name, status, records, run_id):

    conn = get_connection()
    cursor = conn.cursor()

    query = f"""
    INSERT INTO ris360_metadata.file_audit_log
    (file_name, dataset_name, file_status, records_processed, ingestion_time, run_id)
    VALUES
    ('{file_name}', '{dataset_name}', '{status}', {records}, GETDATE(), '{run_id}')
    """

    cursor.execute(query)
    conn.commit()

    cursor.close()
    conn.close()