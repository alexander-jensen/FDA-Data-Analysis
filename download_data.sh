#!/bin/sh

mkdir data
cd data
curl https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_2023-04-20.zip > fda_data.zip
unzip fda_data.zip
rm fda_data.zip
