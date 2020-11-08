import time
import os
import re
from itertools import count
from datetime import datetime
from tqdm import tqdm

import pandas as pd
import numpy as np

import requests

from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.common.exceptions import WebDriverException, NoSuchElementException

import fallen_tools


def inspect_fallen_element(pckg):
    element = pckg['element']
    element_idx = pckg['idx']
    cols = ['rank', 'name', 'parents', 'death date string', 'burial', 'affiliation', 'img_url']
    res = pd.Series(index=cols)
    text_elements = element.text.split('\n')
    if len(text_elements) == 6:
        res['rank'] = text_elements[0]
        res['name'] = text_elements[1]
        res['parents'] = text_elements[2]
        res['death date string'] = text_elements[3]
        res['burial'] = text_elements[4]
        res['affiliation'] = text_elements[5]
    else:
        rank_pos = -1
        name_pos = -1
        parents_pos = -1
        death_date_pos = -1
        burial_pos = -1
        affiliation_pos = -1

        rank_val = '@'
        name_val = '@'
        parents_val = '@'
        death_date_val = '@'
        burial_val = '@'
        affiliation_val = '@'

        for p_text_idx, p_text in enumerate(text_elements):
            if 'מקום' in p_text:
                burial_pos = p_text_idx
                burial_val = text_elements[burial_pos]
            elif (p_text.startswith('בן ')) or (p_text.startswith('בת ')):
                parents_pos = p_text_idx
                parents_val = text_elements[parents_pos]
            elif (p_text.startswith('נפל ')) or (p_text.startswith('נפלה ')):
                death_date_pos = p_text_idx
                death_date_val = text_elements[death_date_pos]
            else:
                pass

        if parents_pos == 1:
            rank_pos = -1
            rank_val = '@'
            name_pos = 0
            name_val = text_elements[name_pos]
        elif parents_pos == 2:
            rank_pos = 0
            rank_val = text_elements[rank_pos]
            name_pos = 1
            name_val = text_elements[name_pos]
        else:  # No parents
            # print("NO PARENTS")
            if death_date_pos == 1:
                rank_pos = -1
                rank_val = '@'
                name_pos = 0
                name_val = text_elements[name_pos]
            elif death_date_pos == 2:
                rank_pos = 0
                rank_val = text_elements[rank_pos]
                name_pos = 1
                name_val = text_elements[name_pos]
            else:
                print("NO PARENTS + NO DEATH")

        if burial_pos == (len(text_elements) - 1):
            affiliation_pos = -1
            affiliation_val = '@'
        elif burial_pos == (len(text_elements) - 2):
            affiliation_pos = len(text_elements) - 1
            affiliation_val = text_elements[affiliation_pos]

        res['rank'] = rank_val
        res['name'] = name_val
        res['parents'] = parents_val
        res['death date string'] = death_date_val
        res['burial'] = burial_val
        res['affiliation'] = affiliation_val

    res['img_url'] = element.find_element_by_tag_name('img').get_attribute('src')

    ret = dict()
    ret['idx'] = element_idx
    ret['data'] = res
    return ret


def fetch_between(start_date, end_date, driver):
    # start_date = '1-1-2015'
    # end_date = '1-1-2021'
    # print(f"Searching from: {start_date} to {end_date}")

    # Fetch website
    search_url = r'https://www.izkor.gov.il/search-by-date/date/{}/{}/d'.format(start_date, end_date)
    driver.get(search_url)
    time.sleep(0.3)

    # Check if "more results" button exists
    retries = 5
    for retry in range(retries):
        button_detected = False
        try:
            element = driver.find_element_by_xpath('//button[text()="הצג עוד תוצאות"]')
            button_detected = True
        except NoSuchElementException:
            pass
        time.sleep(0.5)
        if button_detected:
            break

    # Query data
    start_time = datetime.now()
    if button_detected:
        people_elements_before_click = 0
        for idx in count(0):
            try:
                people_elements = driver.find_elements_by_class_name(r'person-search-card')
                people_elements_before_click = len(people_elements)

                # print(f"[{datetime.now() - start_time}]Clicking {idx}\tCurrent cards: {people_elements_before_click}")
                element.click()
                time.sleep(1)

                people_elements = driver.find_elements_by_class_name(r'person-search-card')
                people_elements_after_click = len(people_elements)

                if people_elements_before_click == people_elements_after_click:
                    # print("DONE.")
                    break

            except WebDriverException:
                print("Element is not clickable")
                break
    else:
        people_elements = driver.find_elements_by_class_name(r'person-search-card')

    return people_elements


def convert_elemnts_to_df(people_elements, multiprocess=True):
    cols = ['rank', 'name', 'parents', 'death date string', 'burial', 'affiliation', 'img_url']
    info = pd.DataFrame(columns=cols, index=range(len(people_elements)))

    inputs = list()
    for idx, element in enumerate(people_elements):
        pckg = dict()
        pckg['element'] = element
        pckg['idx'] = idx
        inputs.append(pckg)

    if multiprocess:
        pool = ThreadPool()
        res = pool.imap_unordered(func=inspect_fallen_element, iterable=inputs)
        time.sleep(0.1)
        for idx in tqdm(range(info.shape[0]), desc='inspecting elements (parallel)'):
            ret = res.next()
            pckg_idx = ret['idx']
            info_sr = ret['data']
            info.loc[pckg_idx] = info_sr
        time.sleep(0.1)
        pool.close()
        pool.join()
        time.sleep(0.1)

    else:
        time.sleep(0.1)
        for idx in tqdm(range(info.shape[0]), desc='inspecting elements (concurrent)'):
            ret = inspect_fallen_element(inputs[idx])
            pckg_idx = ret['idx']
            info_sr = ret['data']
            info.loc[pckg_idx] = info_sr

        time.sleep(0.1)

    return info


def download_img(pckg):
    img_idx = pckg['idx']
    img_dir = pckg['img_dir']
    img_path = os.path.join(img_dir, f'{img_idx}.jpg')
    image_url = pckg['img_url']

    img_data = requests.get(image_url).content
    with open(img_path, 'wb') as handler:
        handler.write(img_data)

    ret = dict()
    ret['img_path'] = img_path
    ret['idx'] = img_idx
    return ret


if __name__ == '__main__':
    path_handler = fallen_tools.Paths_handler()

if __name__ == '__main__':

    section_gather_info = False
    if section_gather_info:
        start_year = 1865
        last_year = 2025
        steps = 10

        global_start_time = datetime.now()
        global_fallen_count = 0
        years_ranges = [(f'1-1-{year}', f'1-1-{year + steps}') for year in np.arange(start_year, last_year, steps)]
        for era_start, era_end in years_ranges:
            driver = webdriver.Chrome()
            era_start_time = datetime.now()
            # print(f"Inspecting era: {era_start} --> {era_end}")
            people_elements = fetch_between(era_start, era_end, driver)
            era_fallen_count = len(people_elements)
            global_fallen_count += era_fallen_count
            info_df = convert_elemnts_to_df(people_elements)
            driver.close()
            fname = f'info_{era_start}_{era_end}.csv'
            oupath = path_handler.export_df(df=info_df, name=fname, folder='info')
            msg = ''
            msg += f'[Exporting to {oupath}]' + '\t'
            msg += f'[Global fallen: {global_fallen_count}]' + '\t'
            msg += f'[Era fallen: {era_fallen_count}]' + '\t'
            msg += f'[Global work time: {datetime.now() - global_start_time}]' + '\t'
            msg += f'[Era work time: {datetime.now() - era_start_time}]' + '\t'
            print(msg)

    section_parse_info_tables = False
    if section_parse_info_tables:
        pattern = r'info_[0-9]+-[0-9]+-[0-9]+_[0-9]+-[0-9]+-[0-9]+\.csv'
        subinfo_files = [f for f in path_handler.get_all_files_from('info') if re.search(pattern, f) is not None]
        info_df = None
        time.sleep(0.1)
        for subinfo_file in tqdm(subinfo_files, desc='loading paths'):
            subinfo_df = path_handler.get_df(subinfo_file, 'info')
            if info_df is None:
                info_df = subinfo_df.copy()
            else:
                info_df = pd.concat([info_df, subinfo_df], ignore_index=True)

        # Special cases
        yoram_likerman_idx = info_df[info_df['name'].str.contains('ליקרמן')].index[0]
        yoram_likerman_death_date_string = r'נפטר ביום כ"ג בשבט תשמ"ג (06.02.1983)'
        info_df.loc[yoram_likerman_idx, 'death date string'] = yoram_likerman_death_date_string

        gyora_myzler_idx = info_df[info_df['name'].str.contains('גיורא מייזלר')].index[0]
        gyora_myzler_death_date_string = r'נפל ביום י"א בתשרי תשל"ד (07.10.1973)'
        info_df.loc[gyora_myzler_idx, 'death date string'] = gyora_myzler_death_date_string

        # Extract death dates
        pattern = r'([0-9]+)\.([0-9]+)\.([0-9]+)'
        deathdf = info_df["death date string"].str.extract(pattern)
        info_df[['death day', 'death month', 'death year']] = deathdf[[0, 1, 2]]
        info_df['death date'] = pd.to_datetime(
            info_df['death day'] + '-' + info_df['death month'] + '-' + info_df['death year'],
            format='%d-%m-%Y')

        # Is valid img
        info_df['valid_img'] = ~info_df['img_url'].str.contains('no_image_defult')

        # Export
        path_handler.export_info_df(info_df)

    section_download_images = True
    if section_download_images:
        info_df = path_handler.get_info_df()
        info_df['img_path'] = '@'
        multiprocess = True

        info_df_with_images = info_df[info_df['valid_img']]
        # Create_inputs
        time.sleep(0.1)
        inputs = list()
        for ridx, r in tqdm(info_df_with_images.iterrows(), total=info_df_with_images.shape[0], desc='preping inputs'):
            pckg = dict()
            pckg['idx'] = ridx
            pckg['img_url'] = r['img_url']
            pckg['img_dir'] = path_handler.get_dir('images')
            inputs.append(pckg)
        time.sleep(0.1)

        if multiprocess:
            pool = ThreadPool()
            res = pool.imap_unordered(func=download_img, iterable=inputs)
            time.sleep(0.1)
            for idx in tqdm(range(len(inputs)), desc='Downloading images (parallel)'):
                ret = res.next()
                info_df.loc[ret['idx'], 'img_path'] = ret['img_path']
            time.sleep(0.1)
            pool.close()
            pool.join()
            time.sleep(0.1)
        else:
            for pckg in tqdm(inputs, desc='Downloading images (concurrent)'):
                ret = download_img(pckg)
                info_df.loc[ret['idx'], 'img_path'] = ret['img_path']

        path_handler.export_info_df(info_df)

    print("END OF CODE")
