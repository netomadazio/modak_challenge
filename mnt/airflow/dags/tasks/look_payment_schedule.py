import logging
import pandas as pd

def _check_values_payment_schedule_backend(payment_schedule_backend_df):
    """
    Checks for various data quality issues in the payment schedule backend DataFrame.
    
    Args:
        payment_schedule_backend_df (pd.DataFrame): DataFrame containing the payment schedule backend data.
    
    Returns:
        list: A list of error messages indicating any data quality issues found.
    The function performs the following checks:
    1. Null Values: Checks for columns containing null values and logs a warning for each column with null values.
    2. Duplicate Values: Checks for duplicate user IDs and logs a warning if any duplicates are found.
    3. Invalid Payment Dates: Checks for payment dates that are not within the valid range (1 to 31) and logs a warning if any invalid dates are found.
    If any exceptions occur during the checks, they are caught and logged as errors.
    """
    
    errors = []
    try:
        null_counts = payment_schedule_backend_df.isnull().sum()
        for column, count in null_counts.items():
            if count > 0:
                error_message = f"Column '{column}' contains {count} null values."
                errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during null value check: {str(e)}")
        logging.error(e)

    try:
        grouped  = payment_schedule_backend_df.groupby('user_id').size()
        duplicate_values = grouped[grouped > 1]
        
        if not duplicate_values.empty:
            error_message = f"The 'payment_schedule_backend' table has duplicate values: \n{duplicate_values}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during duplicate value check: {str(e)}")
        logging.error(e)

    try:
        invalid_payment_date = payment_schedule_backend_df[(payment_schedule_backend_df['payment_date'] < 1) | (payment_schedule_backend_df['payment_date'] > 31)]
        if not invalid_payment_date.empty:
            error_message = f"The 'payment_schedule_backend' table has invalid payment dates: \n{invalid_payment_date}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during invalid payment date check: {str(e)}")
        logging.error(e)

    return errors

def look_for_inconsistencies(**kwargs):
    """
    This function checks for inconsistencies in the payment schedule data.
    
    Args:
        **kwargs: Arbitrary keyword arguments. Expected to contain:
            - payment_schedule_backend (str): URL to the payment schedule backend CSV file.
    
    Raises:
        ValueError: If 'payment_schedule_backend_url' is not provided in kwargs.
        Exception: If there is an error in extracting data to bronze or checking values in the payment schedule backend.
    
    The function performs the following steps:
        1. Imports necessary utilities from the config module.
        2. Retrieves the allowance backend URL from kwargs.
        3. Raises a ValueError if the URL is not provided.
        4. Attempts to read the CSV data from the provided URL into a DataFrame.
        5. Logs and raises any exceptions encountered during the data extraction.
        6. Checks for inconsistencies in the allowance backend data.
        7. Logs and raises any exceptions encountered during the inconsistency check.
    """

    from . import utils
    
    payment_schedule_backend_url = kwargs.get('payment_schedule_backend')

    if not payment_schedule_backend_url:
        raise ValueError("The 'payment_schedule_backend_url' must be provided")

    try:
        payment_schedule_backend_df = utils.get_csv_df(payment_schedule_backend_url)
    except Exception as e:
        logging.critical(f"Critical error in extract_data_to_bronze: {e}")
        raise

    try:
        errors = _check_values_payment_schedule_backend(payment_schedule_backend_df)
    except Exception as e:
        logging.critical(f"Critical error in check_values_allowance_backend: {e}")
        raise

    if errors:
        error_summary = "Errors found during execution:\n" + "\n".join(errors)
        raise Exception(error_summary)
    else:
        logging.info("All checks passed successfully.")


if __name__ == "__main__":
    look_for_inconsistencies()