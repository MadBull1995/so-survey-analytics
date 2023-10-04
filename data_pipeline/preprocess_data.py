import pandas as pd

def normalize_age(age):
    try:
        age = float(age)
    except ValueError:
        return age
    
    if age < 18:
        return 'Under 18 years old'
    elif 18 <= age <= 24:
        return '18-24 years old'
    elif 25 <= age <= 34:
        return '25-34 years old'
    elif 35 <= age <= 44:
        return '35-44 years old'
    elif 45 <= age <= 54:
        return '45-54 years old'
    elif 55 <= age <= 64:
        return '55-64 years old'
    else:
        return '65 years or older'

def handle_missing_values(df, year) -> pd.DataFrame:
    """
    Handle missing values in the DataFrame.
    You can add your custom logic here.
    """

    if year <= 2017:
        df['EmploymentStatus'].fillna('Unknown', inplace=True)
        df['Employment'] = df['EmploymentStatus']
        df = df.drop(columns=['EmploymentStatus'])
    else:
        df['Employment'].fillna('Unknown', inplace=True)
        df['Age'].fillna('Prefer not to say', inplace=True)

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

def process(data: pd.DataFrame, year):
    """
    The 'Data Transformation' stage
    """
    # Handle Missing Values
    data = handle_missing_values(data, year)

    # Remove Duplicates
    data = remove_duplicates(data)

    # Convert Data Types
    data = convert_data_types(data)

    # Normalize Text Columns
    if year > 2017:
        data['Age'] = data['Age'].apply(normalize_age)
        
    # Feature Engineering
    data = feature_engineering(data)

    # Normalize Numeric Columns (if you have any)
    # numeric_columns = ['numeric_column1', 'numeric_column2']
    # data = normalize_numeric(data, numeric_columns)
    print(f"# rows: {len(data)}")
    return data

