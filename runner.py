import pandas as pd
from utility.read_library import read_files, read_db
from utility.validation_library import count_validataion, duplicate_check

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
testcases = pd.read_excel("/Users/admin/PycharmProjects/august_2025_automation/test_automation_framework/test_cases.xlsx")

print("test cases:")
print(testcases)
testcases = testcases.query("execution_ind=='Y' ")

print("test cases after selection exe = 'Y' :")
print(testcases)