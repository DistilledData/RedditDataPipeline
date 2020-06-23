import pandas as pd

df = pd.read_csv('subreddit_list.csv')
with open('subreddit_list.py', 'w') as outfile:
    outfile.write("subreddits = [\n")
    for entry in df['subreddit'].tolist():
        outfile.write("\t{ 'label': \"" + entry + "\", 'value': \"" + entry + "\"},\n")
    outfile.write(']\n')
