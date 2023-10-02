import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import re

def snake_case_to_title_case(snake_str):
    if snake_str.endswith("_id"):
        prefix = snake_str[:-3]  # Everything except the last 3 characters (_id)
        # Some custom workaround
        # TODO: Make this in more elegant way
        if snake_str == "newso_sites_id":
            return "NEWSOSites_id"
        
        return ''.join(word.capitalize() for word in prefix.split('_')) + "_id"
    # Some custom workaround
    # TODO: Make this in more elegant way
    if snake_str == "newso_sites":
        return "NEWSOSites"
    return ''.join(word.capitalize() for word in snake_str.split('_'))

def refactor_column_names_to_title_case(df):
    new_columns = {col: snake_case_to_title_case(col) for col in df.columns}
    df.rename(columns=new_columns, inplace=True)
    return df


def refactor_column_names_to_snake_case(df):
    new_columns = {col: title_case_to_snake_case(col) for col in df.columns}
    df.rename(columns=new_columns, inplace=True)
    return df

def title_case_to_snake_case(s):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def upload_to_db(df: pd.DataFrame, table_name, engine, if_exists='replace'):
    print(f">  uploading: {table_name}")

    # Refactor column names to snake_case before uploading
    df = refactor_column_names_to_snake_case(df)
    
    # Upload table
    print(f">  new rows: {df.shape[0]}")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    

def process_categorical_for_olap(df, col_name, existing_dim_df=None):
    print(f":: {col_name}")
    # Drop NaNs and get unique categories
    unique_categories = set()
    for item_list in df[col_name].dropna():
        for item in item_list.split(';'):
            unique_categories.add(item.strip())

    # If existing_dim_df is provided, filter only new unique categories
    if existing_dim_df is not None:
        existing_dim_df = refactor_column_names_to_title_case(existing_dim_df)
        existing_categories = set(existing_dim_df[col_name])
        new_categories = unique_categories - existing_categories
    else:
        new_categories = unique_categories

    # Create or update the DataFrame for the dimension table
    if existing_dim_df is not None and len(new_categories) > 0:
        next_id = existing_dim_df[f"{col_name}_id"].max() + 1
        new_dim_df = pd.DataFrame(list(new_categories), columns=[col_name])
        new_dim_df[f"{col_name}_id"] = range(next_id, next_id + len(new_dim_df))
        dim_df = new_dim_df
    else:
        dim_df = existing_dim_df if existing_dim_df is not None else pd.DataFrame(list(unique_categories), columns=[col_name])
        if existing_dim_df is None:
            dim_df[f"{col_name}_id"] = range(1, len(dim_df) + 1)

    # Create a mapping from category to ID
    category_to_id = dim_df.set_index(col_name).to_dict()[f"{col_name}_id"]
    # Create link table
    link_list = []
    for idx, row in df.iterrows():
        if pd.notna(row[col_name]):
            for item in row[col_name].split(';'):
                item_id = category_to_id[item.strip()]
                link_list.append({'fact_id': idx, f"{col_name}_id": item_id})

    link_df = pd.DataFrame(link_list)

    # Drop the original column from the main DataFrame
    df = df.drop(columns=[col_name])

    return dim_df, link_df, df


def process_work_mode(df) -> (pd.DataFrame, pd.DataFrame):
    # Create dimension table
    work_modes = df['RemoteWork'].dropna().unique()
    work_mode_df = pd.DataFrame(work_modes, columns=['RemoteWork'])
    work_mode_df['remote_work_id'] = range(1, len(work_mode_df) + 1)
    
    # Create a dictionary for mapping work mode to IDs
    work_mode_to_id = work_mode_df.set_index('RemoteWork').to_dict()['remote_work_id']
    
    # Map the main DataFrame to use remotework_id
    df['remote_work_id'] = df['RemoteWork'].map(work_mode_to_id)
    
    return work_mode_df, df

def process_employment(df) -> (pd.DataFrame, pd.DataFrame):
    # Extract unique employment statuses
    employment_statuses = set()
    for status_list in df['Employment'].str.split(';'):
        if status_list is not np.nan:
            for status in status_list:
                employment_statuses.add(status)

    # Create dimension table
    employment_df = pd.DataFrame(list(employment_statuses), columns=['Employment_Status'])
    employment_df['Employment_Status_ID'] = range(1, len(employment_df) + 1)

    # Create link table
    link_list = []
    for idx, row in df.iterrows():
        if row['Employment'] is not np.nan:
            for status in row['Employment'].split(';'):
                status_id = employment_df.loc[employment_df['Employment_Status'] == status, 'Employment_Status_ID'].iloc[0]
                link_list.append({'Survey_Response_ID': idx, 'Employment_Status_ID': status_id})

    link_df = pd.DataFrame(link_list)

    return employment_df, link_df

def process_pair_of_columns_for_olap(df, col_name_base, existing_dim_df=None):
    print(f":: {col_name_base}")
    # Collect unique categories across both columns
    unique_categories = set()
    
    for suffix in ["HaveWorkedWith", "WantToWorkWith"]:
        col_name = f"{col_name_base}{suffix}"
        for item_list in list(df[col_name].dropna()):
            for item in item_list.split(';'):
                unique_categories.add(item.strip())
    # Filter out existing categories if existing_dim_df is provided
    if existing_dim_df is not None:
        existing_dim_df = refactor_column_names_to_title_case(existing_dim_df)
        existing_categories = set(existing_dim_df[col_name_base])
        new_categories = unique_categories - existing_categories
    else:
        new_categories = unique_categories

    # Create or update the DataFrame for the dimension table
    if existing_dim_df is not None and new_categories:
        next_id = existing_dim_df[f"{col_name_base}_id"].max() + 1
        new_dim_df = pd.DataFrame(list(new_categories), columns=[col_name_base])
        new_dim_df[f"{col_name_base}_id"] = range(next_id, next_id + len(new_dim_df))
        dim_df = pd.concat([existing_dim_df, new_dim_df]).reset_index(drop=True)
    else:
        dim_df = pd.DataFrame(list(new_categories), columns=[col_name_base])
        dim_df[f"{col_name_base}_id"] = range(1, len(dim_df) + 1)
    
    # Create a mapping from category to ID
    category_to_id = dim_df.set_index(col_name_base).to_dict()[f"{col_name_base}_id"]

    link_dfs = {}
    # Create the link tables for each of the two columns
    for suffix in ["HaveWorkedWith", "WantToWorkWith"]:
        col_name = f"{col_name_base}{suffix}"
        link_list = []

        for idx, row in df.iterrows():
            if pd.notna(row[col_name]):
                for item in row[col_name].split(';'):
                    item_id = category_to_id[item.strip()]
                    link_list.append({'fact_id': idx, f"{col_name_base}_id": item_id})

        link_dfs[suffix] = pd.DataFrame(link_list)

    # Drop the original columns from the main DataFrame
    for suffix in ["HaveWorkedWith", "WantToWorkWith"]:
        df = df.drop(columns=[f"{col_name_base}{suffix}"])

    return dim_df, link_dfs, df

def process_special_columns_for_olap(df, col_names):
    unique_categories = set()
    
    # Populate unique_categories and create dimension DataFrame (dim_df)
    for col_name in col_names:
        for item_list in df[col_name].dropna():
            for item in item_list.split(';'):
                unique_categories.add(item.strip())
    
    # Unified column name for the dimension table
    unified_col_name = 'OperatingSystems'
    dim_df = pd.DataFrame(list(unique_categories), columns=[unified_col_name])
    dim_df[f"{unified_col_name}_ID"] = range(1, len(dim_df) + 1)

    # Create a mapping from category to ID
    category_to_id = dim_df.set_index(unified_col_name).to_dict()[f"{unified_col_name}_ID"]

    link_list = []
    # Create the link tables
    for col_name in col_names:
        for idx, row in df.iterrows():
            if pd.notna(row[col_name]):
                for item in row[col_name].split(';'):
                    item_id = category_to_id[item.strip()]
                    link_list.append({'fact_id': idx, f"{unified_col_name}_ID": item_id})

    link_df = pd.DataFrame(link_list)

    # Drop the original columns from the main DataFrame
    for col_name in col_names:
        df = df.drop(columns=[col_name])

    return dim_df, link_df, df


def load(year, processed, db_host_url):
    """
    The 'Data Storage' stage

    """
    engine = create_engine(db_host_url)
    work_mode_dim_df, main_df_updated = process_work_mode(processed)
    upload_to_db(work_mode_dim_df, f"remote_work_dim", engine, 'replace')
    # catagorical_data_cols = [
    #     'Employment',
    #     'CodingActivities',
    #     'LearnCode',
    #     'LearnCodeOnline',
    #     'BuyNewTool',
    #     'NEWSOSites',
    # ]
    df = main_df_updated
    # for col in catagorical_data_cols:
    #     # Fetch existing dimension data from the database
    #     table_name = f"{title_case_to_snake_case(col)}_dim"
    #     try:
    #         existing_dim_data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    #     except Exception as e:
    #         existing_dim_data = pd.DataFrame()
    #     dim_df, link_df, df = process_categorical_for_olap(df, col, None if existing_dim_data.empty else existing_dim_data)
    #     link_table_name = f"{title_case_to_snake_case(col)}_link"
    #     upload_to_db(dim_df, table_name, engine, 'replace')
    #     upload_to_db(link_df, link_table_name, engine)

    catagorical_pairs_cols = [
        'Language',
        'Database',
        'Platform',
        'Webframe',
        'MiscTech',
        'ToolsTech',
        'NEWCollabTools',
        'OfficeStackAsync',
        'OfficeStackSync',
        'AISearch',
        'AIDev',
    ]
    for col in catagorical_pairs_cols:
        table_name = f"{title_case_to_snake_case(col)}_dim"
        try:
            existing_dim_data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        except Exception as e:
            existing_dim_data = pd.DataFrame()
            
        dim_df, link_dfs, df = process_pair_of_columns_for_olap(df, col, None if existing_dim_data.empty else existing_dim_data)
        upload_to_db(dim_df, table_name, engine, 'replace')
        for d in link_dfs:
            table_name_link = f"{title_case_to_snake_case(col)}_{title_case_to_snake_case(d)}_link"
            upload_to_db(link_dfs[d], table_name_link, engine)
        
    dim_df, link_df, df = process_special_columns_for_olap(df, ['OpSysPersonal use', 'OpSysProfessional use'])
    upload_to_db(dim_df, f"op_sys_dim", engine, 'replace')
    upload_to_db(link_df, f"op_sys_link", engine)

    df['SurveyYear'] = year  
    upload_to_db(df, f"survey_facts", engine)
    print("finished loading data :)")