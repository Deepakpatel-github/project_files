import pandas as pd
import re
import argparse
import os
import json

def clean_column_name(name):
    return re.sub(r'\W+', '_', name.lower())

def substitute_environment(table_name, environment):
    return table_name.replace('{Environment}', environment)

def get_ref_tables(joins_df, generate_mode, environment):
    ref_tables = {}
    ref_incr_lat = []
    table_name_column = 'Joined Table Name dbt' if generate_mode == 'dbt' else 'Joined Table Name'
    for _, row in joins_df.iterrows():
        if table_name_column in joins_df.columns and pd.notna(row[table_name_column]):
            table_name = substitute_environment(row[table_name_column], environment)
            common_alias = row['Common Table Alias'] if pd.notna(row['Common Table Alias']) else row['Joined Table Alias']
            ref_tables[common_alias] = (table_name, row.get('Filter Specification2', ''))
        if 'Primary Table Name' in joins_df.columns and pd.notna(row['Primary Table Name']):
            primary_table_name = 'Primary Table Name dbt' if generate_mode == 'dbt' else 'Primary Table Name'
            primary_table_name = substitute_environment(row[primary_table_name], environment)
            common_alias = row['Primary Table Alias']
            filter_specification = 'Filter Specification dbt' if generate_mode == 'dbt' else 'Filter Specification'
            ref_tables[common_alias] = (primary_table_name, row.get(filter_specification, ''))
            
        if 'latest_code' in joins_df.columns:
            ref_incr_lat.append(row['latest_code'])
    return ref_tables,ref_incr_lat

def generate_ref_ctes(ref_tables, environment,ref_incr_lat):
    ctes = ""

    for alias, (table_name, filter_spec) in ref_tables.items():
        filter_spec = substitute_environment(filter_spec, environment)
        cte = f"{alias} AS (\n"
        cte += f"  SELECT *\n"
        cte += f"  FROM {table_name}\n"
        if filter_spec and pd.notna(filter_spec):
            cte += f"  {filter_spec}\n"
        cte += "),\n\n"
        ctes += cte
    
    if len(ref_incr_lat) > 1:
        ctes += f"  {ref_incr_lat[-1]}\n"
    
    return ctes

# def generate_ref_ctes_increment(ctes,ref_tables, environment,alias_name):
#     '''
#     adding incremental logic at the end of the sanitized layer
#     '''
#     if alias_name in ref_tables:
#         filter_spec = substitute_environment(ref_tables[alias_name][1], environment)
#         if filter_spec and pd.notna(filter_spec):
#             ctes += f"  {filter_spec}\n"
#     return ctes


def generate_source_cte(alias, joins, mappings, ref_tables, source_system, data_zone,model_primary_key,environment):
    cte = f"{alias} AS (\n"
    cte += "  SELECT\n"
    group_by_columns = []

    for _, mapping in mappings.iterrows():
        
        if source_system in mapping.index:
            alias_name = f"{joins['Primary Table Override'].iloc[0] if pd.notna(joins['Primary Table Override'].iloc[0]) else joins['Primary Table Alias'].iloc[0]}"
            target_column = clean_column_name(mapping['Target Column'])
            source_column = mapping[source_system]
            merge_rule = mapping.get('Merge Rule', '').strip() if data_zone == "conformed" else ''

            if pd.isna(source_column):
                source_column = 'NULL'
            if data_zone == "conformed":
                if merge_rule == 'group by':
                    group_by_columns.append(target_column)
                    cte += f"    {alias_name}.{source_column} AS {target_column},\n"
                elif merge_rule in ['max', 'min']:
                    cte += f"    {merge_rule}({source_column}) AS {target_column},\n"
                elif merge_rule.startswith('['):  # Complex JSON rule
                    try:
                        rule = json.loads(merge_rule)
                        coalesce_parts = []
                        for item in sorted(rule, key=lambda x: x['precedence']):
                            # import pdb; pdb.set_trace()
                            comparison_column = item['comparison_column']
                            comparison_value = item['comparison_value']
                            operator = item.get('operator', 'max')
                            comparison_column_param = f"COALESCE(latest_{alias_name}.{comparison_column}, {alias_name}.{comparison_column})" 
                            comparison_column_result = f"COALESCE(latest_{alias_name}.{source_column},{alias_name}.{source_column})"
                            coalesce_parts.append(f"\t{operator}(CASE WHEN {comparison_column_param} = '{comparison_value}' THEN {comparison_column_result} END) \n")
                            # coalesce_parts.append(f"\t{operator}(CASE WHEN {comparison_column} = '{comparison_value}' THEN {joins['Primary Table Override'].iloc[0] if pd.notna(joins['Primary Table Override'].iloc[0]) else joins['Primary Table Alias'].iloc[0]}.{source_column} END) \n")
                        cte += f"    COALESCE({', '.join(coalesce_parts)}) AS {target_column},\n"
                    except json.JSONDecodeError:
                        print(f"Warning: Invalid JSON for merge rule of column {target_column}")
                        cte += f"    {source_column} AS {target_column},\n"
                else:
                    cte += f"    {source_column} AS {target_column},\n"
            else:
                cte += f"    {source_column} AS {target_column},\n"
    cte = cte.rstrip(',\n') + "\n"
    cte += f"  FROM {joins['Primary Table Override'].iloc[0] if pd.notna(joins['Primary Table Override'].iloc[0]) else joins['Primary Table Alias'].iloc[0]}\n"
    
    
    for _, join in joins.iterrows():
        if 'Join Type' in joins.columns:
            if join['Join Type'].upper() in ['JOIN', 'LEFT JOIN']:
                common_alias = join['Common Table Alias'] 
                join_alias = join['Joined Table Alias']
                cte += f"  {join['Join Type'].upper()} {common_alias} as {join_alias}\n"
                if 'Join Condition' in joins.columns:
                    cte += f"    ON {join['Join Condition']}\n"

    
    if data_zone == "conformed" and group_by_columns:
        # Join condition has been added to tack
        cte += f"    LEFT JOIN {alias_name}_existing latest_{alias_name} on {alias_name}.{model_primary_key}=latest_{alias_name}.{model_primary_key} \n"
        # end of new condition
        cte += "  GROUP BY\n"
        cte += ",\n".join(f"    {alias}.{col}" for col in group_by_columns)
        cte += "\n"

    # if data_zone != 'conformed':
    #     for i in len(ref_tables):
    #         cte += generate_ref_ctes_increment(cte,ref_tables,environment,alias_name)
    cte += "),\n\n"
    return cte

def generate_dbt_model(excel_file, generate_mode, environment):
    try:
        # Read the Excel sheets
        summary_df = pd.read_excel(excel_file, sheet_name='Model')
        joins_df = pd.read_excel(excel_file, sheet_name='Table Setup')
        mappings_df = pd.read_excel(excel_file, sheet_name='Column Mappings')
        
        # Get model information from the Model sheet
        data_zone = summary_df['Table Zone'].iloc[0]
        table_primary_key = summary_df['Model Primary Key Name'].iloc[0]
        table_materialized = summary_df['Materialized'].iloc[0]
        model_name = summary_df['Model Name'].iloc[0]
        source_systems = summary_df['Source Systems'].iloc[0].split(',')
        
        # Print column names for debugging
        print("Joins DataFrame columns:", joins_df.columns)
        print("Mappings DataFrame columns:", mappings_df.columns)

        # Get distinct source system aliases
        source_aliases = joins_df['Source System Alias'].unique()

        # Get reference tables
        ref_tables, ref_incr_lat = get_ref_tables(joins_df, generate_mode, environment)

        model = "{{\n" 
        model += "   config(\n"
        model += f"   materialized='{table_materialized}',\n"
        model += f"   unique_key=['{table_primary_key}']\n"
        model += "   )\n"
        model += "}}\n\n" 

        # Add the WITH clause
        model += "WITH \n\n"
    
        # Generate CTEs for reference tables (now unique)
        model += generate_ref_ctes(ref_tables, environment,ref_incr_lat)
    
        # Generate CTEs for each source system
        for alias in source_aliases:
            joins = joins_df[joins_df['Source System Alias'] == alias]
            source_system = joins['Source System'].iloc[0]
            model += generate_source_cte(alias, joins, mappings_df, ref_tables, source_system, data_zone,table_primary_key,environment)
            # model += generate_source_cte(alias, joins, mappings_df, ref_tables, source_system, data_zone)
    
        # Generate UNION ALL statement
        model += "combined_sources AS (\n"
        for i, alias in enumerate(source_aliases):
            model += f"  SELECT * FROM {alias}\n"
            if i < len(source_aliases) - 1:
                model += "  UNION ALL\n"
        model += "),\n\n"
    
        # Standard - section for PK generation
        model += "combined_sources_pk as (\n"
        model += " select \n"
        if data_zone == "sanitized":
            model += "{{ dbt_utils.generate_surrogate_key([ 'source_system_name', 'source_system_record_id']) }} as %s \n" %(table_primary_key)
            model += ",*\n"
        else:
            model += "*\n"
        model += "from combined_sources\n"
        model += "),\n\n"

        # Get all column names from mappings with their ordinal positions and data types
        all_columns = [(row['Target Column'], row['Ordinal Position'], row['Data Type']) for _, row in mappings_df.iterrows()]
        
        # Add audit columns
        audit_columns = [
            (f"{data_zone}_created_batch_id", 1000, "VARCHAR"),
            (f"{data_zone}_created_date_time", 1001, "TIMESTAMP_NTZ"),
            (f"{data_zone}_updated_batch_id", 1002, "VARCHAR"),
            (f"{data_zone}_updated_date_time", 1003, "TIMESTAMP_NTZ"),
            (f"{data_zone}_record_version", 1004, "INTEGER")
        ]
        all_columns.extend(audit_columns)

        # Add Section for fetching audit fields
        model += "pre_final as (\n"
        model += "select\n"
        model += "  src.*,\n"
        model += f"  nvl(pcr.{data_zone}_created_batch_id, '{{{{ invocation_id }}}}') as {data_zone}_created_batch_id,\n"
        model += f"  '{{{{ invocation_id }}}}' as {data_zone}_updated_batch_id,\n"
        model += f"  nvl(pcr.{data_zone}_created_date_time, sysdate()) as {data_zone}_created_date_time,\n"
        model += f"  sysdate() as {data_zone}_updated_date_time,\n"
        model += f"  case when pcr.{data_zone}_record_version is null then 1 else (pcr.{data_zone}_record_version + 1) end as {data_zone}_record_version\n"
        model += "from\n"
        model += "  combined_sources_pk as src\n"
        model += "left join\n"
        model += f"  {{{{ this }}}} as pcr on src.{table_primary_key} = pcr.{table_primary_key}\n"
        model += "),\n\n"

        # Generate the final SELECT statement with explicit column names, ordered by ordinal position
        model += "final as (\n"
        model += "select\n"
        if data_zone == 'sanitized':
            model += f"  {table_primary_key},\n"
        for column, _, data_type in sorted(all_columns, key=lambda x: x[1]):
            model += f"  {column}::{data_type} as {column},\n"
        model = model.rstrip(',\n') + "\n"  # Remove the last comma
        model += "from pre_final\n"
        model += "),\n\n"
        model += "{{\n"
        model += "    tag_inserts_and_updates(\n"
        model += "      source_data = 'SELECT * FROM final',\n"
        model += f"      unique_id = ['{table_primary_key}']\n"
        model += "    )\n"
        model += "}}"

        return model, model_name

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None, None

def main():
    parser = argparse.ArgumentParser(description="Generate DBT or Snowflake models")
    parser.add_argument("--generate-type", choices=["single", "batch"], required=True, help="Generate a single model or batch of models")
    parser.add_argument("--model-name", help="Name of the model to generate (required if generate-type is 'single')")
    parser.add_argument("--generate-mode", choices=["dbt", "snowflake"], required=True, help="Generate model for DBT or Snowflake")
    parser.add_argument("--environment", required=True, help="Environment to substitute in table names")
    parser.add_argument("--directory-name", help="base folder path")
    args = parser.parse_args()

    if args.generate_type == "single" and not args.model_name:
        parser.error("--model-name is required when --generate-type is 'single'")

    if args.generate_type == "single":
        excel_file = f"{args.model_name}.xlsx"
        if not os.path.exists(excel_file):
            print(f"Error: Excel file '{excel_file}' not found.")
            return

        dbt_model, model_name = generate_dbt_model(excel_file, args.generate_mode, args.environment)
        if dbt_model and model_name:
            output_file = f"{model_name.lower()}.{'sql' if args.generate_mode == 'dbt' else 'snowflake'}"
            with open(output_file, 'w') as f:
                f.write(dbt_model)
            print(f"{args.generate_mode.upper()} model has been generated and saved as '{output_file}'")
        else:
            print(f"Failed to generate the {args.generate_mode.upper()} model. Please check the error message above.")
    else:
        # Implement batch processing logic here
        path = args.directory_name
        for (dirpath, subdir, file) in os.walk(path):
            for f in file:
                if '.xlsx' in f:
                    excel_file = os.path.join(dirpath, f)
                    if not os.path.exists(excel_file):
                        print(f"Error: Excel file '{excel_file}' not found.")
                        return

                    dbt_model, model_name = generate_dbt_model(excel_file, args.generate_mode, args.environment)
                    if dbt_model and model_name:
                        output_file = f"{dirpath}\{model_name.lower()}.{'sql' if args.generate_mode == 'dbt' else 'snowflake'}"
                        with open(output_file, 'w') as f:
                            f.write(dbt_model)
                        print(f"{args.generate_mode.upper()} model has been generated and saved as '{output_file}'")
                    else:
                        print(f"Failed to generate the {args.generate_mode.upper()} model. Please check the error message above.")
        
if __name__ == "__main__":
    main()
