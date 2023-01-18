import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import json
import gzip
import os 
from tqdm import tqdm
from pprint import pprint

def read_repo_metadata(filepath):
    # read metadata info in json format
    try:
        with open(filepath, 'r') as f:
            res = json.load(f)
        return res
    except OSError:
        print('')


def filter_repo_metadata(m,urls={}):
    # m is a dictionary of all retrieved repo metadata
    # m should be filtered with a dictionary of urls (distinct)
    filtered_m = m 
    matched_urls = set()

    activity_record = {}
    activity_record['url'], activity_record['forks_count'], activity_record['watchers_count'], activity_record['subscribers_count'] = [], [], [], []
    activity_record['full_name'] = []
    for i in tqdm(filtered_m):
        try:
            keys = filtered_m[i].keys()
            target_keys = ['html_url','forks_count','watchers_count','subscribers_count','full_name']
            for key in target_keys:
                if key not in keys:
                    continue
            if filtered_m[i]['html_url'] in matched_urls:
                continue
            
            matched_urls.add(filtered_m[i]['html_url'])
            activity_record['url'].append(filtered_m[i]['html_url'])
            activity_record['forks_count'].append(filtered_m[i]['forks_count'])
            activity_record['watchers_count'].append(filtered_m[i]['watchers_count'])
            activity_record['subscribers_count'].append(filtered_m[i]['subscribers_count'])
            activity_record['full_name'].append(filtered_m[i]['full_name'])
        except AttributeError as ae:
            continue
        except TypeError as te:
            continue
        # pprint(activity_record)
    return filtered_m, activity_record

def read_gzip(filepath):
    # read .json.gz file
    # file source: Martin
    try:
        output = {}
        output['paper_id'], output['source'], output['date'],output['url'] = [], [], [], []
        with gzip.open(filepath, 'rb') as f:
            res = json.loads(f.read().decode('utf-8'))
        for paper_id in tqdm(res, total=len(res)):
            github_links_list = res[paper_id]
            for link in github_links_list:
                source_from, date, url = link['source'], link['date'], link['url']
                output['paper_id'].append(paper_id)
                output['source'].append(source_from)
                output['date'].append(date)
                output['url'].append(url)
        return output
    except OSError:
        print('')


def convert_dict2csv(d, new_csv_filepath=None):
    df = pd.DataFrame.from_dict(d)
    if new_csv_filepath is not None:
        df.to_csv(new_csv_filepath, index=False)
    return df


def filter_distinct_e(d):
    # d is the dictionary read from the json.gz file
    unique_urls = {}    # unique_urls is a dictionary with keys being distinct urls and values being the indexes  
    for i, url in tqdm(enumerate(d['url'])):
        if url not in unique_urls:
            unique_urls[url] = [i]
        else:
            unique_urls[url].append(i)
    # produce the first appeared positions for all distinct urls
    first_pos = []
    for url in tqdm(unique_urls):
        first_pos.append(unique_urls[url][0])
    # produce the occurence count of each distinct url
    occur_count = {}
    for url in tqdm(unique_urls):
        occur_count[url] = len(unique_urls[url])
    return unique_urls, first_pos, occur_count


def plot_dis(data, x, name_basis, kind='hist', bins=10, binrange=(10,310)):
    sns.set(style='darkgrid')
    if kind == 'hist':
        plot = sns.displot(data, x=x, aspect=2, kind=kind,bins=bins, binrange=binrange)
    elif kind == 'kde':
        plot = sns.displot(data,x=x, kind=kind)
    elif kind == 'both':
        plot = sns.displot(data,x=x,bins=bins, kde=True)
    plot.figure.savefig('./figures/{}_{}_{}_{}_{}.png'.format(name_basis,kind, x,bins,binrange))


data_parentdir = './data/'
json_filepath = os.path.join(data_parentdir,'repos_from_martin_metadata_enhanced.json')
metadata = read_repo_metadata(json_filepath)
# filter_repo_metadata(metadata)
# print(len(metadata))
# pprint(metadata['12599'].keys())

gzip_filepath ='/media/sf_UnknownData/UnknownDataProject/From Others/Martin/cs.json.gz'
res = read_gzip(gzip_filepath)
# res = convert_dict2csv(res, 'github_links_from_martin.csv')

urls_dict, first_pos, occur_count = filter_distinct_e(res)
print(len(urls_dict))
print(len(first_pos))
print(first_pos[:10])

filtered_metadata, act_rec = filter_repo_metadata(metadata, urls=urls_dict)
print(len(act_rec))
occur_count_l = sorted(list(occur_count.items()), key=lambda x:x[1], reverse=True)
pprint(occur_count_l[:10])
df_occur_count = pd.DataFrame.from_dict({'url':list(occur_count.keys()), 'count': list(occur_count.values())})
df_act_rec = pd.DataFrame.from_dict(act_rec)
pprint(df_act_rec.head(10))
print(len(df_act_rec))

readme_json_file = os.path.join(data_parentdir, 'repo_readme_from_martin.json')
readme_metadata = read_repo_metadata(readme_json_file)
print(len(readme_metadata))

# plot_dis(df_occur_count,x='count', bins=310, binrange=(10,320), name_basis='link_occurence_distribution', kind='kde')
# plot_dis(df_occur_count,x='count', bins=312, binrange=(8,320), name_basis='link_occurence_distribution', kind='both')
plot_dis(df_act_rec, x='forks_count',name_basis='forks_occurence_distribution',kind='both')
plot_dis(df_act_rec, x='subscribers_count',name_basis='subscribers_occurence_distribution',kind='both')
plot_dis(df_act_rec, x='watchers_count',name_basis='watchers_occurence_distribution',kind='both')
