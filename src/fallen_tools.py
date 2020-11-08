import os
import pandas as pd


def clear_path(path):
    if not os.path.exists(path):
        os.makedirs(path)


class Paths_handler:
    def __init__(self):
        self.src_dir = os.path.dirname(__file__)
        self.work_dir = os.path.dirname(self.src_dir)
        self.info_dir = os.path.join(self.work_dir, 'info')
        self.statistics_dir = os.path.join(self.work_dir, 'statistics')
        self.images_dir = os.path.join(self.work_dir, 'images')

        self.paths = dict()
        self.paths['root'] = self.work_dir
        self.paths['src'] = self.src_dir
        self.paths['info'] = self.info_dir
        self.paths['statistics'] = self.statistics_dir
        self.paths['images'] = self.images_dir

        for k, path in self.paths.items():
            clear_path(path)

        self.infodf_path = os.path.join(self.info_dir, 'info_df.csv')

    def get_dir(self, folder):
        return self.paths.get(folder)

    def export_df(self, df, name, folder):
        outpath_dir = self.paths.get(folder)
        oupath = os.path.join(outpath_dir, name)
        df.to_csv(oupath, encoding='utf-8-sig')
        return oupath

    def get_df(self, name, folder, date_cols=list()):
        path = os.path.join(self.paths.get(folder), name)
        df = pd.read_csv(path, index_col=0, parse_dates=date_cols)
        return df

    def get_all_files_from(self, folder):
        return os.listdir(self.paths.get(folder))

    def export_info_df(self, df):
        name = 'info_df.csv'
        folder = 'info'
        self.export_df(df, name, folder)

    def get_info_df(self):
        name = 'info_df.csv'
        folder = 'info'
        df = self.get_df(name, folder)
        if 'death date' in df:
            df['death date'] = pd.to_datetime(df['death date'])

        intcols = ['death day', 'death month', 'death year']
        for col in intcols:
            if col in df:
                df[col] = df[col].astype(int)
        return df
