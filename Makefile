.PHONY: clean data lint requirements sync_data_to_s3 sync_data_from_s3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = [OPTIONAL] your-bucket-for-syncing-data (do not include 's3://')
PROFILE = default
PROJECT_NAME = analyzing_brazil_presidential_elections
PYTHON_INTERPRETER = python3

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python Dependencies
requirements: test_environment
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Make brazil complete dataset: result of joining states raw data by year
brazil_data: requirements
	$(PYTHON_INTERPRETER) src/data/1_make_brazil_dataset.py

## Make presidential dataset: result of filtering brazil dataset to retrieve
## only presidential votes
presidential_data: brazil_data
	$(PYTHON_INTERPRETER) src/data/2_make_presidential_dataset.py

## Make dataset with 1st turn votes and only regular votes: no transit votes, no
## outside Brazil votes.
first_turn_data: presidential_data
	$(PYTHON_INTERPRETER) src/data/3_make_first_turn_dataset.py

## Make percentual votes dataset
percentual_data: first_turn_data
	$(PYTHON_INTERPRETER) src/data/4_make_percentual_dataset.py

## Make datasets of chosen parties with cod_mun and percentual_votos columns
parties_data: percentual_data
	$(PYTHON_INTERPRETER) src/data/5_make_parties_dataset.py

## Make series data joining years data by party
series_data: parties_data
	$(PYTHON_INTERPRETER) src/data/6_make_series_dataset.py

## Make latlon data joining series and latlon data
latlon_data: series_data
	$(PYTHON_INTERPRETER) src/data/7_make_series_latlon_dataset.py

## Normalize series_latlon using data
normalized_latlon_data: latlon_data
	$(PYTHON_INTERPRETER) src/data/8_1_make_normalized_latlon_dataset.py

## Normalize series_latlon using EPSG:4674
epsg_4674_latlon_data: latlon_data
	$(PYTHON_INTERPRETER) src/data/8_2_make_epsg_4674_latlon_dataset.py

## Normalize series_latlon using EPSG:4326
epsg_4326_latlon_data: latlon_data
	$(PYTHON_INTERPRETER) src/data/8_3_make_epsg_4326_latlon_dataset.py

## Make Moran's Index dataset
moran: series_data
	$(PYTHON_INTERPRETER) src/data/9_make_moran_datasets_and_plots.py

## Make hierarchical clustering
cluster_data: normalized_latlon_data epsg_4674_latlon_data epsg_4326_latlon_data
	$(PYTHON_INTERPRETER) src/data/10_make_dendrograms_and_cluster_datasets.py

## Make plots using hierarchical clustering
cluster_plots: cluster_data
	$(PYTHON_INTERPRETER) src/data/11_make_cluster_plots.py

## Make Moran's Index dataset and plots with HDI data
moran_hdi:
	$(PYTHON_INTERPRETER) src/data/12_make_hdi_moran_datasets_and_plots.py

## Make hierarchical clustering
cluster_hdi_data:
	$(PYTHON_INTERPRETER) src/data/13_make_hdi_dendrograms_and_cluster_datasets.py

## Make plots using hierarchical clustering
cluster_hdi_plots: cluster_hdi_data
	$(PYTHON_INTERPRETER) src/data/14_make_hdi_cluster_plots.py

## Make final dataset without interims
data:
	$(PYTHON_INTERPRETER) src/data/make_dataset.py

profile:
	$(PYTHON_INTERPRETER) src/data/make_profile_dataset.py

single_profile: profile
	$(PYTHON_INTERPRETER) src/data/make_single_profile_dataset.py

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8
lint:
	flake8 src

## Upload Data to S3
sync_data_to_s3:
ifeq (default,$(PROFILE))
	aws s3 sync data/ s3://$(BUCKET)/data/
else
	aws s3 sync data/ s3://$(BUCKET)/data/ --profile $(PROFILE)
endif

## Download Data from S3
sync_data_from_s3:
ifeq (default,$(PROFILE))
	aws s3 sync s3://$(BUCKET)/data/ data/
else
	aws s3 sync s3://$(BUCKET)/data/ data/ --profile $(PROFILE)
endif

## Set up python interpreter environment
create_environment:
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
ifeq (3,$(findstring 3,$(PYTHON_INTERPRETER)))
	conda create --name $(PROJECT_NAME) python=3
else
	conda create --name $(PROJECT_NAME) python=2.7
endif
		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
else
	$(PYTHON_INTERPRETER) -m pip install -q virtualenv virtualenvwrapper
	@echo ">>> Installing virtualenvwrapper if not already installed.\nMake sure the following lines are in shell startup file\n\
	export WORKON_HOME=$$HOME/.virtualenvs\nexport PROJECT_HOME=$$HOME/Devel\nsource /usr/local/bin/virtualenvwrapper.sh\n"
	@bash -c "source `which virtualenvwrapper.sh`;mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER)"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"
endif

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
