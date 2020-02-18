import pandas as pd

column_types = {
'Id': 'object',
'BIRTHDATE': 'category'
}

optimized = pd.read_csv('test.csv',dtype=column_types)
print(optimized)
