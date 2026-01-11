import pandas as pd
from utility.read_library import read_files, read_db
from utility.validation_library import count_validataion, duplicate_check
import openpyxl

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
testcases = pd.read_excel(r"/Users/admin/PycharmProjects/test_automation_framework_ws/configs/test_cases.xlsx")

print("test cases:")
print(testcases)
testcases = testcases.query("execution_ind=='Y' ")

print("test cases after selection exe = 'Y' :")
print(testcases)

for ind,row in testcases.iterrows():

    if row['source_type'] == 'database':
        source_df = read_db(query = row['source_query'],
                            creds_file=r"/Users/admin/PycharmProjects/test_automation_framework_ws/configs/creds_file.xlsx",
                            env=row['database_type'])
    else:
        source_df = read_files(file_path=row['source_path'], file_type= row['source_type'])

    if row['target_type'] == 'database':
        target_df = read_db(query=row['target_query'],
                            creds_file="/old/test_automation_framework/creds_file.xlsx",
                            env='qa')
    else:
        target_df = read_files(file_path=row['target_path'], file_type=row['target_type'])

    print("source Dataframe")
    print(source_df.head(5))
    print("="*100)
    print("target Dataframe")
    print(target_df.head(5))
    print("=" * 100)
    print(row['validations'])
    print("row['validations'].split(',')", row['validations'].split(','))
    for val in row['validations'].split(','):
        if val == 'count':
            print("Count validation has started")
            count_validataion(source_df, target_df)
            print("Count validation is complete")
            print("=" * 100)

        elif val == 'duplicate':
            print("duplicate validation has started")
            duplicate_check(target_df=target_df, pkey_column=row['primary_key'])
            print("duplicate validation is complete")
            print("=" * 100)


df = pd.DataFrame()