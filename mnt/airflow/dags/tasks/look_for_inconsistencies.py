import logging
import pandas as pd
from datetime import datetime, timedelta

def _check_values_allowance_backend(allowance_events_df, allowance_backend_df):
    """
    Check if the values in the 'allowance_backend' table are consistent with the 'allowance_events' table.

    Args:
        allowance_events_df (pd.DataFrame): The 'allowance_events' table.
        allowance_backend_df (pd.DataFrame): The 'allowance_backend' table.

    Returns:
        pd.DataFrame: The inconsistencies found between the two tables.
    """
    allowance_events_df['event_timestamp'] = pd.to_datetime(allowance_events_df['event_timestamp'])
    allowance_events_df['event_timestamp'] = allowance_events_df['event_timestamp'].astype(int) // 10**9
    
    errors = []
    try:
        allowance_events_df = allowance_events_df.sort_values(by='event_timestamp') \
                                                    .drop_duplicates(subset='user_id', keep='last')
        
        allowance_backend_df = allowance_backend_df[allowance_backend_df['status'] == 'enabled']

        allowance_events_backend_df = pd.merge(allowance_events_df, allowance_backend_df, left_on='user_id', right_on='uuid', how='inner')

        inconsistent_values = allowance_events_backend_df[(allowance_events_backend_df['allowance_scheduled_frequency'] != allowance_events_backend_df['frequency']) 
                                                                | (allowance_events_backend_df['allowance_scheduled_day'] != allowance_events_backend_df['day'])
                                                                | (allowance_events_backend_df['user_id'].isna())
                                                                | (allowance_events_backend_df['uuid'].isna())]
    
        if not inconsistent_values.empty:
            error_message = f"The 'allowance_backend' table has inconsistent values: \n{inconsistent_values}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during allowance backend check: {str(e)}")
        logging.error(e)
    
    return errors

def _check_values_payment_schedule_backend(allowance_backend_df, payment_schedule_backend_df):
    """
    Check if the values in the 'payment_schedule_backend' table are consistent with the 'allowance_backend' table.

    Args:
        allowance_backend_df (pd.DataFrame): The 'allowance_backend' table.
        payment_schedule_backend_df (pd.DataFrame): The 'payment_schedule_backend' table.

    Returns:
        pd.DataFrame: The inconsistencies found between the two tables.
    """

    allowance_backend_payment_df = pd.merge(allowance_backend_df, payment_schedule_backend_df, left_on='uuid', right_on='user_id', how='inner')
    errors = []
    try:
        inconsistent_values_disabled_payments = allowance_backend_payment_df[(allowance_backend_payment_df['status'] == 'disabled') & (allowance_backend_payment_df['payment_date'].notna())]
        
        if not inconsistent_values_disabled_payments.empty:
            error_message = f"The 'payment_schedule_backend' table has inconsistent disabled values: \n{inconsistent_values_disabled_payments}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during disabled payments check: {str(e)}")
        logging.error(e)

    allowance_backend_payment_df = allowance_backend_payment_df[allowance_backend_payment_df['status'] == 'enabled']

    try:
        inconsistent_values_on_payments = allowance_backend_payment_df[(allowance_backend_payment_df['next_payment_day'] != allowance_backend_payment_df['payment_date'])]
        
        if not inconsistent_values_on_payments.empty:
            error_message = f"The 'payment_schedule_backend' table has different values than 'allowance_backend': \n{inconsistent_values_on_payments}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during enabled payments check: {str(e)}")
        logging.error(e)
    
    return errors

def look_for_inconsistencies(**kwargs):
    """
    This function checks for inconsistencies between allowance events, allowance backend, and payment schedule backend data.
    Keyword Arguments:
    kwargs -- Dictionary containing the following keys:
        'allowance_events' (str): URL to fetch allowance events data.
        'allowance_backend' (str): URL to fetch allowance backend data.
        'payment_schedule_backend' (str): URL to fetch payment schedule backend data.
    Raises:
    ValueError: If any of 'allowance_events_url', 'allowance_backend_url', or 'payment_schedule_backend_url' are not provided.
    Exception: If there is a critical error in fetching data or checking values.
    The function performs the following steps:
    1. Fetches data from the provided URLs.
    2. Checks values between allowance events and allowance backend data.
    3. Checks values between allowance backend and payment schedule backend data.
    """

    from . import utils
    
    allowance_events_url = kwargs.get('allowance_events')
    allowance_backend_url = kwargs.get('allowance_backend')
    payment_schedule_backend_url = kwargs.get('payment_schedule_backend')

    if not allowance_events_url or not allowance_backend_url or not payment_schedule_backend_url:
        raise ValueError("The 'allowance_events_url', 'allowance_backend_url', and 'payment_schedule_backend_url' must be provided")

    try:
        allowance_events_df = utils.fetch_json_from_url(allowance_events_url)
        allowance_backend_df = utils.get_csv_df(allowance_backend_url)
        payment_schedule_backend_df = utils.get_csv_df(payment_schedule_backend_url)
    except Exception as e:
        logging.critical(f"Critical error in get data: {e}")
        raise

    try:
        errors_allowance = _check_values_allowance_backend(allowance_events_df, allowance_backend_df)
    except Exception as e:
        logging.critical(f"Critical error in check_values_allowance_backend: {e}")
        raise

    try:
        errors_payment = _check_values_payment_schedule_backend(allowance_backend_df, payment_schedule_backend_df)
    except Exception as e:
        logging.critical(f"Critical error in check_values_payment_schedule_backend: {e}")
        raise

    errors = []
    errors.extend(errors_allowance)
    errors.extend(errors_payment)

    if errors:
        error_summary = "Errors found during execution:\n" + "\n".join(errors)
        raise Exception(error_summary)
    else:
        logging.info("All checks passed successfully.")


if __name__ == "__main__":
    look_for_inconsistencies()