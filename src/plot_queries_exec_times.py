#!/usr/bin/python3

import sys
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    print('Usage: plot_queries_exec_times.py <execution_times>')
    exit(-1)

exec_times_file = sys.argv[1]

exec_times = {}
exec_times['Query'] = []
exec_times['Type'] = []
exec_times['Time'] = []

# exec_times_dict = {}
# for query in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
#     exec_times_dict[query] = {}


with open(exec_times_file) as fp:
    line = fp.readline()
    while line:
        if line != '\n':
            tokens = line.split(':')
            name = tokens[0]
            time = float(tokens[1].split()[0])

            name_tokens = name.split('_')
            query = name_tokens[1]
            query = 'Q' + query

            query_type = name_tokens[2]
            if query_type == 'sql':
                query_format = name_tokens[3]
                query_format = '_'.join([query_type, query_format])
            else:
                query_format = query_type

            exec_times['Query'].append(query)
            exec_times['Type'].append(query_format)
            exec_times['Time'].append(time)

            # exec_times_dict[query][query_format] = time

        line = fp.readline()

print(exec_times)
df_exec_times = pd.DataFrame.from_dict(exec_times)
print(df_exec_times)

sns.set_theme(style='whitegrid')

g = sns.catplot(
    data=df_exec_times, kind='bar',
    x='Query', y='Time', hue='Type',
    ci='sd', palette='dark', alpha=.6, height=6
)
g.despine(left=True)
g.set_axis_labels('Queries', 'Execution Time (sec)')
g.legend.set_title('')
plt.title('')
# plt.show()
plt.savefig(f"queries_exec_times.png", bbox_inches="tight")


# print(exec_times_dict)
# df_exec_times_dict = pd.DataFrame.from_dict(exec_times_dict, orient='index')
# print(df_exec_times_dict)
