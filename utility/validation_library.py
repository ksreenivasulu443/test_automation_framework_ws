from pandasql import sqldf
from utility.reporting import write_output

def count_validataion(source, target):
    source_count = source.shape[0]
    target_count = target.shape[0]
    if source_count == target_count:
        print("count is matching")
        status = 'PASS'
        write_output(validation_type='count_validataion', status=status, details=f'source count is {source_count} and target count is {target_count}')
    else:
        print(f"count is not mtaching between source and target. Source count is {source_count} and target count is {target_count}. Diff is {source_count-target_count}")
        status = 'FAIL'
        write_output(validation_type='count_validataion', status=status,details=f'source count is {source_count} and target count is {target_count}')

def duplicate_check(target_df,pkey_column):
    original_count = target_df.shape[0] #== 10
    count_after_drop = target_df[pkey_column].drop_duplicates().shape[0] # 10
    if original_count == count_after_drop:
        print("no duplicates")
    else:
        print("duplicates present")


def duplicate_check_sql(target_df, pkey_column):
    failed_df = sqldf(f"select {pkey_column}, count(1) from target_df group by {pkey_column} having count(1)>1 ")
    print(failed_df)
    if  failed_df.empty:
        print("no duplicates")
    else:
        print("duplicates present")

def null_check(target_df,null_column):
    null_column_list = str(null_column).split(',')
    for col in null_column_list:
        null_rows = target_df[target_df[col].isnull()]
        print("null rows", null_rows)
        #null_rows = sqldf(f"select * from target_df where {null_column} is null")
        if null_rows.shape[0]>0:
            print(f" {col} Null rows present")
        else:
            print(f" {col} No nulls presennt")

def data_compare_1(source_df,target_df):
    failed = source_df.compare(target_df)
    if failed.shape[0]>0:
        print("data is not present")
        print(failed)
    else:
        print("data is matching")

def data_compare(source_df, target_df):
    query = """select * from source_df except select * from target_df
                union all
                select * from target_df except select * from source_df"""
    failed = sqldf(query)
    if failed.shape[0]>0:
        print("data is not matching")
        print(failed)
    else:
        print("data is matching")

def check_out_of_range(target_df, column, min_val, max_val ):
    query = f"""
    SELECT * FROM target_df
    WHERE  {column} between {min_val} and  {max_val}
    """
    invalid_df = sqldf(query)
    if invalid_df.shape[0]>0:
        print("invalid records present")
        print(invalid_df)
    else:
        print("All records within the range")



# profile_Load ==> file --> raw --> bronze --> silver
#
# file --> raw
# raw --> bronze
# bronze -- > silver
# silver --> gold
#
# transaction_Load





