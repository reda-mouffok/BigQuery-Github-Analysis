#!/bin/sh

docker build . -t github_analysis:1.0.0
docker run --name github_analysis_container -v credentials:/var/Github-Analysis-Project/credentials -v result:/var/Github-Analysis-Project/result -it github_analysis:1.0.0