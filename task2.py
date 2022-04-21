import pandas as pd
data = pd.read_csv("subject1.csv")
print(data)
data[['first','last']] = data['company_name'].str.split(' ', expand=True)
data.drop_duplicates(subset ="first",keep = 'last',inplace=True)
print(data[['id','company_name','address','start_date']])

