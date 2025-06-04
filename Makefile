#
# Copyright 2024 Clumio, a Commvault company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# SHELL ensures more consistent behavior between macOS and Linux.
SHELL=/bin/bash

test_reports := build/test_reports/py

.PHONY: *


clean:
	rm -rf build .mypy_cache .coverage *.egg-info dist code/.coverage

build:
	rm -rf build/lambda build/clumio_velero_restore.zip build/clumio-eks-ebs-restore-deploy-cft-sa.yaml
	mkdir -p build/lambda
	mkdir -p build/lambda/utils
	cp code/*.py build/lambda/
	cp -r code/utils/* build/lambda/utils
	pip install -r requirements.txt -t build/lambda/
	cd build/lambda && zip -r ../clumio_velero_restore.zip .
	cp code/clumio-eks-ebs-restore-deploy-cft-sa.yaml build/
