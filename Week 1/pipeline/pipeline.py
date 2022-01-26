
import sys
import pandas as pd

df = pd.DataFrame({"COl1":[1,2,3,4],"Col2":['a','b','c','d']})
print(df.head)

day = sys.argv[1]

print(f"Pandas loaded and processed successfully for day {day}")