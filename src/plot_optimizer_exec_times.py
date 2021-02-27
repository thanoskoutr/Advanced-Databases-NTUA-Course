#!/usr/bin/python3

import sys
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    print('Usage: plot_optimizer_exec_times.py <execution_times>')
    exit(-1)

exec_times_file = sys.argv[1]

exec_times = {}
exec_times['Optimizer'] = []
exec_times['Time'] = []


with open(exec_times_file) as fp:
    line = fp.readline()
    while line:
        if line.startswith('Time with choosing join type disabled'):
            optimizer = 'no optimizer'
            tokens = line.split('is ')
            time = float(tokens[1].split('sec')[0])

            exec_times['Optimizer'].append(optimizer)
            exec_times['Time'].append(time)

        elif line.startswith('Time with choosing join type enabled'):
            optimizer = 'with optimizer'
            tokens = line.split('is ')
            time = float(tokens[1].split('sec')[0])

            exec_times['Optimizer'].append(optimizer)
            exec_times['Time'].append(time)

        line = fp.readline()


print(exec_times)
df_exec_times = pd.DataFrame.from_dict(exec_times)
print(df_exec_times)

# sns.set_theme(style='whitegrid')

g = sns.catplot(
    data=df_exec_times, kind='bar',
    x='Optimizer', y='Time',
    ci='sd', palette='deep', alpha=.6, height=6
)
g.despine(left=True)
g.set_axis_labels('Joins', 'Execution Time (sec)')
plt.title('')
# plt.show()
plt.savefig(f"optimizer_exec_times.png", bbox_inches="tight")
