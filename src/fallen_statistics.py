import time
import os
import re
from itertools import count
from datetime import datetime
from tqdm import tqdm

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import fallen_tools

if __name__ == '__main__':
    path_handler = fallen_tools.Paths_handler()

if __name__ == '__main__':
    info_df = path_handler.get_info_df()

    section_plot_timline = True
    if section_plot_timline:
        timeline_df = info_df.groupby(by=['death year']).count()['name']

        plt.plot(timeline_df.index, timeline_df)

        section_add_prime_ministers = False
        if section_add_prime_ministers:
            pm_df = path_handler.get_df(name='prime ministers.csv', folder='info',
                                        date_cols=['term start'])
            for ridx, r in pm_df.iterrows():
                year = r['term start'].year + (r['term start'].month / 12.0)
                label = f'{r["name"]} ({year:>.1f})'
                plt.axvline(x=year, label=label, c='red')

        plt.xticks(np.arange(min(timeline_df.index), max(timeline_df.index) + 1, 5.0))
        plt.yticks(np.arange(min(timeline_df), max(timeline_df) + 1, 50.0))
        plt.legend()
        plt.grid()
        # plt.show()
        figformat = 'svg'
        plt.savefig(os.path.join(path_handler.statistics_dir, f'timeline.{figformat}'), format=figformat,
                    dpi=1200, bbox_inches='tight')
        plt.show()
        plt.close()
