import logging
import pandas as pd

def _check_values_allowance_backend(allowance_backend_df):
    """
    Checks the allowance_backend DataFrame for various data quality issues.
    
    Args:
        allowance_backend_df (pd.DataFrame): The DataFrame containing allowance backend data.
    
    Returns:
        list: A list of error messages indicating any data quality issues found.
    
    The function performs the following checks:
    1. Null Values: Checks for columns containing null values and logs a warning for each column with null values.
    2. Duplicate Values: Checks for duplicate 'uuid' values and logs a warning if any duplicates are found.
    3. Invalid Payment Dates: Checks for 'next_payment_day' values that are not between 1 and 31 and logs a warning if any invalid dates are found.
    4. Invalid Frequencies: Checks for 'frequency' values that are not in the allowed list ['daily', 'biweekly', 'weekly', 'monthly'] and logs a warning if any invalid frequencies are found.
    5. Invalid Days: Checks for 'day' values that are not in the allowed list ['fifteenth_day', 'first_day', 'friday', 'monday', 'tuesday', 'thursday', 'saturday', 'sunday', 'daily', 'wednesday'] and logs a warning if any invalid days are found.
    Each check is wrapped in a try-except block to catch and log any exceptions that occur during the check.
    """
    
    errors = []
    try:
        null_counts = allowance_backend_df.isnull().sum()
        for column, count in null_counts.items():
            if count > 0:
                error_message = f"Column '{column}' contains {count} null values."
                errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during null value check: {str(e)}")
        logging.error(e)

    try:
        grouped  = allowance_backend_df.groupby('uuid').size()
        duplicate_values = grouped[grouped > 1]
        
        if not duplicate_values.empty:
            error_message = f"The 'allowance_backend' table has duplicate values: \n{duplicate_values}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during duplicate value check: {str(e)}")
        logging.error(e)

    try:
        invalid_payment_date = allowance_backend_df[(allowance_backend_df['next_payment_day'] < 1) | (allowance_backend_df['next_payment_day'] > 31)]
        if not invalid_payment_date.empty:
            error_message = f"The 'allowance_backend' table has invalid payment dates: \n{invalid_payment_date}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during invalid payment date check: {str(e)}")
        logging.error(e)

    try:
        invalid_frequency = allowance_backend_df[~allowance_backend_df['frequency'].isin(['daily', 'biweekly', 'weekly', 'monthly'])]
        if not invalid_frequency.empty:
            error_message = f"The 'allowance_backend' table has invalid frequencies: \n{invalid_frequency}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during invalid frequency check: {str(e)}")
        logging.error(e)
    
    try:
        list_days = ['fifteenth_day', 'first_day', 'friday', 'monday', 'tuesday',
        'thursday', 'saturday', 'sunday', 'daily', 'wednesday']
        invalid_day = allowance_backend_df[~allowance_backend_df['day'].isin(list_days)]
        if not invalid_day.empty:
            error_message = f"The 'allowance_backend' table has invalid day: \n{invalid_day}"
            errors.append(error_message)
    except Exception as e:
        errors.append(f"Error during invalid day check: {str(e)}")
        logging.error(e)

    return errors

def look_for_inconsistencies(**kwargs):
    """
    This function checks for inconsistencies in the allowance backend data.
    
    Args:
        **kwargs: Arbitrary keyword arguments. Expects 'allowance_backend' key with the URL to the allowance backend CSV.
    
    Raises:
        ValueError: If 'allowance_backend' URL is not provided in kwargs.
        Exception: If there is an error in extracting data to bronze or checking values in the allowance backend.
    
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
    
    allowance_backend_url = kwargs.get('allowance_backend')

    if not allowance_backend_url:
        raise ValueError("The 'allowance_backend_url'must be provided")

    try:
        allowance_backend_df = utils.get_csv_df(allowance_backend_url)
    except Exception as e:
        logging.critical(f"Critical error in extract_data_to_bronze: {e}")
        raise

    try:
        errors = _check_values_allowance_backend(allowance_backend_df)
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