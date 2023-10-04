import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import re

def refactor_column_names_to_snake_case(df):
    new_columns = {col: title_case_to_snake_case(col) for col in df.columns}
    df.rename(columns=new_columns, inplace=True)
    return df

def title_case_to_snake_case(s):
    # Convert TitleCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    # Replace whitespaces with underscores
    snake_case = snake_case.replace(' ', '_')
    
    return snake_case

def upload_to_db(df: pd.DataFrame, table_name, engine, if_exists='replace'):
    print(f">  uploading: {table_name}")
    
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
        # existing_dim_df = refactor_column_names_to_title_case(existing_dim_df)
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

def process_professional_tech(df) -> (pd.DataFrame, pd.DataFrame):
    column_name = "professional_tech"
    stacked_df = (df[column_name]
        .str.split(';', expand=True)
        .stack()
        .reset_index(level=1, drop=True)
        .rename(column_name))
    
    unique_techs = pd.DataFrame(stacked_df.drop_duplicates().reset_index(drop=True), columns=[column_name])
    unique_techs['professional_tech_id'] = unique_techs.index
    
    dim_table = unique_techs[['professional_tech_id', column_name]]
    
    stacked_df = stacked_df.reset_index()
    link_table = pd.merge(stacked_df, dim_table, on=column_name, how='left')
    link_table.rename(columns={'index': 'response_id'}, inplace=True)
    
    # Adjust response_id to align with real IDs
    link_table['response_id'] = link_table['response_id'] + 1
    
    link_table = link_table.drop(columns=[column_name])
    df = df.drop(columns=[column_name])

    return dim_table, link_table, df

def process_dev_type(df) -> (pd.DataFrame, pd.DataFrame):
    column_name = "dev_type"
    # First, split by commas to get separate professions
    stacked_df = (df[column_name]
        .str.split(',', expand=True)
        .stack()
        .reset_index(level=1, drop=True)
        .rename(column_name))

    # Then, take the part before the first semicolon in each string
    stacked_df = stacked_df.str.split(';', n=1).str[0]
    
    # Normalize text: convert to lowercase, replace '-' and '_' with '', and strip leading/trailing whitespace
    stacked_df = (stacked_df.str.lower()
                  .str.replace('-', '')
                  .str.replace('_', '')
                  .str.strip())
    
    unique_techs = pd.DataFrame(stacked_df.drop_duplicates().reset_index(drop=True), columns=[column_name])
    
    unique_techs['dev_type_id'] = unique_techs.index
    
    dim_table = unique_techs[['dev_type_id', column_name]]
    
    stacked_df = stacked_df.reset_index()
    link_table = pd.merge(stacked_df, dim_table, on=column_name, how='left')
    link_table.rename(columns={'index': 'response_id'}, inplace=True)
    
    # Adjust response_id to align with real IDs
    link_table['response_id'] = link_table['response_id'] + 1

    link_table = link_table.drop(columns=[column_name])
    df = df.drop(columns=[column_name])

    return dim_table, link_table, df

def process_work_mode(df) -> (pd.DataFrame, pd.DataFrame):
    # Create dimension table
    work_modes = df['remote_work'].dropna().unique()
    work_mode_df = pd.DataFrame(work_modes, columns=['remote_work'])
    work_mode_df['remote_work_id'] = range(1, len(work_mode_df) + 1)
    
    # Create a dictionary for mapping work mode to IDs
    work_mode_to_id = work_mode_df.set_index('remote_work').to_dict()['remote_work_id']
    
    # Map the main DataFrame to use remote_work_id
    df['remote_work_id'] = df['remote_work'].map(work_mode_to_id)
    
    return work_mode_df, df

def process_employment(df) -> (pd.DataFrame, pd.DataFrame):
    # Extract unique employment statuses
    employment_statuses = set()
    for status_list in df['employment'].str.split(';'):
        if status_list is not np.nan:
            for status in status_list:
                employment_statuses.add(status)

    # Create dimension table
    employment_df = pd.DataFrame(list(employment_statuses), columns=['employment_status'])
    employment_df['employment_status_id'] = range(1, len(employment_df) + 1)

    # Create link table
    link_list = []
    for idx, row in df.iterrows():
        if row['employment'] is not np.nan:
            for status in row['employment'].split(';'):
                status_id = employment_df.loc[employment_df['employment_status'] == status, 'employment_status_id'].iloc[0]
                link_list.append({'survey_response_id': idx, 'Employment_status_id': status_id})

    link_df = pd.DataFrame(link_list)

    return employment_df, link_df

def process_pair_of_columns_for_olap(df, col_name_base, existing_dim_df=None):
    print(f":: {col_name_base}")
    # Collect unique categories across both columns
    unique_categories = set()
    suffixes = ["_have_worked_with", "_want_to_work_with"]
    
    for suffix in suffixes:
        col_name = f"{col_name_base}{suffix}"
        if col_name not in df.columns:
            print(f"!! Warning: Column '{col_name}' not found in DataFrame.")
            return None, None, df
        for item_list in list(df[col_name].dropna()):
            for item in item_list.split(';'):
                unique_categories.add(item.strip())
    
    if existing_dim_df is not None:
        # Use the existing dimension DataFrame if provided
        dim_df = existing_dim_df
        existing_categories = set(existing_dim_df[col_name_base])
        new_categories = unique_categories - existing_categories
        
        # Add new categories to dim_df
        if new_categories:
            next_id = existing_dim_df[f"{col_name_base}_id"].max() + 1
            new_dim_df = pd.DataFrame(list(new_categories), columns=[col_name_base])
            new_dim_df[f"{col_name_base}_id"] = range(next_id, next_id + len(new_dim_df))
            dim_df = pd.concat([existing_dim_df, new_dim_df]).reset_index(drop=True)
    else:
        dim_df = pd.DataFrame(list(unique_categories), columns=[col_name_base])
        dim_df[f"{col_name_base}_id"] = range(1, len(dim_df) + 1)
    
    # Create a mapping from category to ID
    category_to_id = dim_df.set_index(col_name_base).to_dict()[f"{col_name_base}_id"]
    link_dfs = {}
    # Create the link tables for each of the two columns
    for suffix in suffixes:
        col_name = f"{col_name_base}{suffix}"
        link_list = []
        for idx, row in df.iterrows():
            if pd.notna(row[col_name]):
                for item in row[col_name].split(';'):
                    item_id = category_to_id.get(item.strip())
                    if item_id is not None:
                        link_list.append({'fact_id': idx, f"{col_name_base}_id": item_id})
        link_dfs[suffix] = pd.DataFrame(link_list)

    # Drop the original columns from the main DataFrame
    for suffix in suffixes:
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
    unified_col_name = 'operating_system'
    dim_df = pd.DataFrame(list(unique_categories), columns=[unified_col_name])
    dim_df[f"{unified_col_name}_id"] = range(1, len(dim_df) + 1)

    # Create a mapping from category to ID
    category_to_id = dim_df.set_index(unified_col_name).to_dict()[f"{unified_col_name}_id"]

    link_list = []
    # Create the link tables
    for col_name in col_names:
        for idx, row in df.iterrows():
            if pd.notna(row[col_name]):
                for item in row[col_name].split(';'):
                    item_id = category_to_id[item.strip()]
                    link_list.append({'fact_id': idx, f"{unified_col_name}_id": item_id})

    link_df = pd.DataFrame(link_list)

    # Drop the original columns from the main DataFrame
    for col_name in col_names:
        df = df.drop(columns=[col_name])

    return dim_df, link_df, df

def load(processed, db_host_url):
    """
    The 'Data Storage' stage

    """
    
    # Refactor column names to snake_case before uploading
    df = refactor_column_names_to_snake_case(processed)

    # DB Engine init
    engine = create_engine(db_host_url)
    
    # ----------------------------------------------------------------------------
    #                           remote_work
    # ----------------------------------------------------------------------------
    work_mode_dim_df, df = process_work_mode(df)
    upload_to_db(work_mode_dim_df, "remote_work_dim", engine, 'replace')

    # ----------------------------------------------------------------------------
    #                           professional_tech
    # ----------------------------------------------------------------------------
    pro_tech_dim_df, pro_tech_link_df, df = process_professional_tech(df)
    upload_to_db(pro_tech_dim_df, "professional_tech_dim", engine)
    upload_to_db(pro_tech_link_df, "professional_tech_link", engine)

    # ----------------------------------------------------------------------------
    #                               dev_type
    # ----------------------------------------------------------------------------
    dev_type_dim_df, dev_type_link_df, df = process_dev_type(df)
    upload_to_db(dev_type_dim_df, "dev_type_dim", engine)
    upload_to_db(dev_type_link_df, "dev_type_link", engine)

    # ----------------------------------------------------------------------------
    #                                op_sys
    # ----------------------------------------------------------------------------
    dim_df, link_df, df = process_special_columns_for_olap(df, ['op_sys_personal_use', 'op_sys_professional_use'])
    upload_to_db(dim_df, "op_sys_dim", engine, 'replace')
    upload_to_db(link_df, "op_sys_link", engine, 'replace')

    # ----------------------------------------------------------------------------
    #                       Iterating catagorical columns
    # ----------------------------------------------------------------------------
    catagorical_data_cols = [
        'employment',
        'coding_activities',
        'learn_code',
        'learn_code_online',
        'buy_new_tool',
        'newso_sites',
    ]
    for col in catagorical_data_cols:
        # Fetch existing dimension data from the database
        table_name = f"{col}_dim"
        try:
            existing_dim_data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        except Exception as e:
            existing_dim_data = pd.DataFrame()
        dim_df, link_df, df = process_categorical_for_olap(df, col, None if existing_dim_data.empty else existing_dim_data)
        link_table_name = f"{col}_link"
        upload_to_db(dim_df, table_name, engine, 'replace')
        upload_to_db(link_df, link_table_name, engine)

    # ----------------------------------------------------------------------------
    #           Iterating catagorical columns with have/want_work_with
    # ----------------------------------------------------------------------------
    catagorical_pairs_cols = [
        'language',
        'database',
        'platform',
        'webframe',
        'misc_tech',
        'tools_tech',
        'new_collab_tools',
        'office_stack_async',
        'office_stack_sync',
        'ai_search',
        'ai_dev',
    ]
    for col in catagorical_pairs_cols:
        table_name = f"{col}_dim"
        try:
            existing_dim_data = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        except Exception as e:
            existing_dim_data = pd.DataFrame()

        dim_df, link_dfs, df = process_pair_of_columns_for_olap(df, col, None if existing_dim_data.empty else existing_dim_data)
        if dim_df is not None:
            upload_to_db(dim_df, table_name, engine, 'replace')
        if link_dfs is not None:
            for d in link_dfs:
                table_name_link = f"{col}{d}_link"
                upload_to_db(link_dfs[d], table_name_link, engine, 'replace')

    # ----------------------------------------------------------------------------
    #                           Main survey facts table
    # ---------------------------------------------------------------------------- 
    upload_to_db(df, "survey_facts", engine)

    print("finished loading data :)")