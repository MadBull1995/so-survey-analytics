import pandas as pd
# from sklearn.preprocessing import StandardScaler, LabelEncoder

def handle_missing_values(df) -> pd.DataFrame:
    """
    Handle missing values in the DataFrame.
    You can add your custom logic here.
    """

    df['Employment'].fillna('Unknown', inplace=True)

    # df.fillna(df.median(), inplace=True)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.
    """
    df.drop_duplicates(inplace=True)
    if "Q120" in df.columns:
        df.drop(columns=["Q120"],inplace=True)
    return df

def convert_data_types(df) -> pd.DataFrame:
    """
    Convert data types for specific columns if necessary.
    """
    # Converting column to category data type
    # df['column_name'] = df['column_name'].astype('category')
    return df

def normalize_text(df, text_columns) -> pd.DataFrame:
    """
    Normalize text data in given columns.
    """
    for col in text_columns:
        df[col] = df[col].str.lower()
    return df

def feature_engineering(df) -> pd.DataFrame:
    """
    Create new features or metrics that could be valuable for the analysis.
    """
    # Your custom feature engineering logic here
    return df

# def normalize_numeric(df, numeric_columns):
#     """
#     Normalize numeric columns using Standard Scaler.
#     """
#     scaler = StandardScaler()
#     df[numeric_columns] = scaler.fit_transform(df[numeric_columns])
#     return df

def process(data: pd.DataFrame):
    """
    The 'Data Transformation' stage
    """
    # Handle Missing Values
    data = handle_missing_values(data)

    # Remove Duplicates
    data = remove_duplicates(data)

    # Convert Data Types
    data = convert_data_types(data)

    # Normalize Text Columns (if you have any)
    # text_columns = ['text_column1', 'text_column2']
    # data = normalize_text(data, text_columns)

    # Feature Engineering
    data = feature_engineering(data)

    # Normalize Numeric Columns (if you have any)
    # numeric_columns = ['numeric_column1', 'numeric_column2']
    # data = normalize_numeric(data, numeric_columns)
    print(f"# rows: {data.count()['ResponseId']}")
    return data
