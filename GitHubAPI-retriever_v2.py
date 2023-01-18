import requests
import os
import json
from pprint import pprint
import pandas as pd
import time
from tqdm import tqdm 
import gzip 
import copy
from bs4 import BeautifulSoup

# token_file = "../GitHub_token_11022023.txt"
# github_links_csv = "../GitHub Repos/Github_datasets_repo_metadata.csv"
# github_links_json_from_martin = '/media/sf_UnknownData/UnknownDataProject/From Others/Martin/cs.json.gz'

token_file = "./GitHub_token_11022023.txt"
github_links_csv = "./Github_datasets_repo_metadata.csv"
github_links_json_from_martin = './From Others/Martin/cs.json.gz'
github_links_csv_from_martin = './github_links_from_martin.csv'

### get github links from csv file 
with open(token_file, 'r') as f:
    ACCESS_TOKEN = f.read()
github_links = pd.read_csv(github_links_csv)['In-Paper Links'].to_numpy()

### get github links from json file
def extract_github_links(gzip_file):
    with gzip.open(gzip_file, 'rb') as f:
        d = json.loads(f.read().decode('utf-8'))
    github_urls = []
    for paper_id in d:
        github_links_list = d[paper_id]
        for metadata_github_link in github_links_list:
            github_urls.append(metadata_github_link['url'])
    return github_urls

# github_links_from_martin = extract_github_links(github_links_from_martin)


### method to get owner and repo info for a list of github repo urls 
def extract_owner_repo_from_github_links(link_list):
    if type(link_list) == str:
        link = link_list
        link = link.split('/')
        if link[-1] == '':
            link.pop()
        if link[-1][-4:] == '.git':
            link[-1] = link[-1][:-4]
        owners, repos = link[-2], link[-1]
    elif len(link_list) > 0:    
        owners, repos = [], []
        for link in link_list:
            link = link.split('/')
            if link[-1] == '':
                link.pop()
            if link[-1][-4:] == '.git':
                link[-1] = link[-1][:-4]
            owners.append(link[-2])
            repos.append(link[-1])
    return owners, repos

def remove_duplicates(owners, repos):
    onr = set()
    new_owners, new_repos = [], []
    for owner, repo in zip(owners, repos):
        if (owner, repo) not in onr:
            onr.add((owner, repo))
            new_owners.append(owner)
            new_repos.append(repo)
    return new_owners, new_repos



headers = {'Authorization': 'Token '+ACCESS_TOKEN,
        'Accept': 'application/vnd.github+json'}

### method to retrieve a repo metadata using GitHub API
def retrieve_repo_metadata(owners, repos, save_to=None):
    # retrieve repo metadata
    retrieved_output = {}
    print(os.path.exists(save_to))
    if os.path.exists(save_to):
        with open(save_to, 'r') as f:
            retrieved_output = json.load(f)
    print(len(retrieved_output))
    output = {}
    irre = {}
    offset = 0 
    if len(retrieved_output) > 0: 
        print('Retrieved  '+str(len(retrieved_output)))
        output = copy.deepcopy(retrieved_output)
        offset = len(output)
    for i, (owner, repo) in tqdm(enumerate(zip(owners[offset:], repos[offset:])), total=len(owners)-offset):
        res = requests.get('https://api.github.com/repos/{}/{}'.format(owner, repo),
                headers=headers)
        # print(res.status_code)
        if res.status_code == 200:
            # print(res.status_code)
            output[i+offset] = res.json()
        else:
            res = requests.get('https://github.com/{}/{}'.format(owner, repo),
                    headers=headers)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                new_url = soup.find('meta', property='og:url')['content']
                owner, repo = extract_owner_repo_from_github_links(new_url)
                res = requests.get('https://api.github.com/repos/{}/{}'.format(owner, repo),
                        headers=headers)
                if res.status_code == 200:
                    output[i+offset] = res.json()
                else:
                    # print(owner, repo)
                    output[i+offset] = '' 
                    irre[i+offset] = (owner, repo)
            else:
                output[i+offset] = ''
                irre[i+offset] = (owner, repo)
        time.sleep(1)
        
        if len(output)%100 == 0:
            if save_to is not None:
                with open(save_to, 'w') as f:
                    json.dump(output, f, indent=4,ensure_ascii=False)
    
    if save_to is not None:
        with open(save_to, 'w') as f:
            json.dump(output, f,indent=4, ensure_ascii=False)
    return output, irre

### method to retrieve a repo content metadata using GitHub API 
def retrieve_repo_content_metadata(owners, repos, save_to=None):
    # retrieve repo content metadata
    repo_content = {}
    offset = 0
    if os.path.exists(save_to):
        with open(save_to, 'r') as f:
            repo_content = json.load(f)
            offset = len(repo_content)
    irre1 = {}
    for i, (owner, repo) in enumerate(tqdm(zip(owners[offset:], repos[offset:]), total=len(owners)-offset)):
        res = requests.get('https://api.github.com/repos/{}/{}/contents/'.format(owner, repo), headers=headers)
        if res.status_code == 200:
            repo_content[i+offset] ={'fullname':'{}/{}'.format(owner,repo), 
                    'content':res.json()}
        else:
            res = requests.get('https://github.com/{}/{}'.format(owner,repo),
                    headers=headers)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text,'html.parser')
                new_url = soup.find('meta', property='og:url')['content']
                owner, repo = extract_owner_repo_from_github_links(new_url)
                res = requests.get('https://api.github.com/repos/{}/{}/contents/'.format(owner,repo),headers=headers)
                if res.status_code == 200:
                    repo_content[i+offset] ={'fullname':'{}/{}'.format(owner,repo),
                            'content':res.json()}
                else:
                    repo_content[i+offset] = ''
                    irre1[i+offset] = (owner,repo)
            else:
                repo_content[i+offset] = ''
                irre1[i+offset] = (owner, repo)
        time.sleep(1)

        if len(repo_content)%100 == 0:
            if save_to is not None:
                with open(save_to, 'w') as f:
                    json.dump(repo_content,f,indent=4,ensure_ascii=False)
    
    if save_to is not None:
        with open(save_to, 'w') as f:
            json.dump(repo_content, f, indent=4, ensure_ascii=False)
    return repo_content, irre1


### method to retrieve README content for a github repo using GitHub API 
def retrieve_repo_readme(owners, repos, save_to=None):
    readmes = {}
    irre = {}
    offset = 0

    if os.path.exists(save_to):
        with open(save_to, 'r') as f:
            readmes = json.load(f)
            offset = len(readmes)

    for i, (owner, repo) in enumerate(tqdm(zip(owners[offset:], repos[offset:]), total=len(owners)-offset)):
        res = requests.get('https://api.github.com/repos/{}/{}/readme'.format(owner, repo), headers=headers)
        if res.status_code == 200:
            readme = requests.get(res.json()['download_url'], headers=headers)
            if readme.status_code == 200:
                readmes[i+offset] ={'fullname':'{}/{}'.format(owner,repo), 
                        'content':readme.text}
            else:
                readmes[i+offset] = ''
                irre[i+offset] = (owner, repo)
        else:
            res = requests.get('https://github.com/{}/{}'.format(owner,repo),
                    headers=headers)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                new_url = soup.find('meta',property='og:url')['content']
                owner, repo = extract_owner_repo_from_github_links(new_url)
                res = requests.get('https://api.github.com/repos/{}/{}/readme'.format(owner, repo), headers=headers)
                if res.status_code == 200:
                    readme = requests.get(res.json()['download_url'],headers=headers)
                    if readme.status_code == 200:
                        readmes[i+offset] ={'fullname':'{}/{}'.format(owner,repo), 
                            'content':readme.text}
                    else:
                        readmes[i+offset] = ''
                        irre[i+offset] = (owner, repo)
                else:
                    readmes[i+offset] = ''
                    irre[i+offset] = (owner, repo)
            else:
                readmes[i+offset] = ''
                irre[i+offset] = (owner, repo)
        time.sleep(1)
        if len(readmes)%100 == 0:
            if save_to is not None:
                with open(save_to, 'w') as f:
                    json.dump(readmes,f,indent=4,ensure_ascii=False)
        
    if save_to is not None:
        with open(save_to, 'w') as f:
            json.dump(readmes, f, indent=4, ensure_ascii=False)
    return readmes, irre

# with open('irre.json', 'w') as f:
#     json.dump(irre, f, indent=4, ensure_ascii=False)

# with open('irre1.json', 'w') as f:
#     json.dump(irre1, f, indent=4, ensure_ascii=False)


def run():
    github_links_from_martin = extract_github_links(github_links_json_from_martin)
    print(len(github_links_from_martin))
    # print(len(github_links))
    owners, repos = extract_owner_repo_from_github_links(github_links_from_martin)
    print(len(owners))
    owners, repos = remove_duplicates(owners, repos)
    print(len(owners))
    save_to_metadata = 'repos_from_martin_metadata_enhanced.json'
    save_to_content_metadata = 'repos_content_from_martin_metadata_enhanced.json'
    save_to_readme = 'readme_from_martin_enhanced.json'
    _, irre = retrieve_repo_content_metadata(owners, repos, save_to=save_to_content_metadata)
    # pprint(irre)
    # _, irre = retrieve_repo_readme(owners, repos, save_to=save_to_readme)
    with open('irre_content_from_martin_enhanced.json', 'w') as f:
        json.dump(irre,f,indent=4,ensure_ascii=False)


run()
