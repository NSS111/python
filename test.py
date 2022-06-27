import pandas as pd 
import csv
import os
import glob

# create function to read csv
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe

# create function to read pipedelimited
def extract_from_pipe(file_to_process): 
    dataframe = pd.read_csv(file_to_process,delimiter='|') 
    return dataframe

#call extract function 
def extract():
    extracted_data = pd.DataFrame(columns=['product_name','quality','material_id','worth','source_name']) 
    path = os.getcwd()
    csv_files = glob.glob(path+"\**\*.*.*",recursive = True)
    #for csv files
    for csvfile in csv_files:
      with open(csvfile) as f:
        header =f.read()
        if header.find(",") !=-1:  #header contains comma
          sourcename = os.path.dirname(csvfile)[-13:]
          extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
          extracted_data.assign(source_name = sourcename)
        else:
          #for pipe delimited
          extracted_data = extracted_data.append(extract_from_pipe(csvfile), ignore_index=True) 
          extracted_data.assign(source_name = sourcename)
    return extracted_data 

def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile,index=False)

extracted_data = extract()
print(extracted_data)
load('output.csv',extracted_data)