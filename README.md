# where-is-cs-data

This repo contains data and supporting code for the paper "Finding Computer Science Research Data - Exploratory Studies on Zenodo, arXiv and GitHub". 

## Data 
### Manual annotation data
The manual annotation results are given in CSV files, in `./manual_annotation_results/`.

### Processed publication with github links data
This data is the result of preprocessing the [unarXive dataset](https://doi.org/10.5281/zenodo.4313164) version 2020, given in `./data/cs.json.gz`. 

### Retrieved GitHub repo metadata
We retrieved three types of metadata regarding repos: the general metadata in `./data/metadata.json.gz`, the root directory metadata in `./data/content_metadata.json.gz`and the README in `./data/readme.json.gz`. We orginally save these three in JSON format and have uploaded them in compressed format. 

## Code
For reproducing GitHub metadata retrieval process, you can modify the `run()` function inside `./GitHubAPI_retriever_v2.py` for corresponding type of metadata, then run the file (uncomment the `run()` line).

For producing the statistical results, an uncleaned version is given in jupyter notebook, in `./metadata_analysis.ipynb`. A cleaned version will soon be updated.

The preprocessing of unarXive dataset is given in `./github_link_extraction`.

## License
This repo is distributed under [GNU General Public License, Version 3](https://www.gnu.org/licenses/gpl-3.0.html), in `./LICENSE`.


