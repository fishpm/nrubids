#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  9 11:32:24 2022

@author: patrick
"""

from sys import argv
import json
import os

# Input from command line
argumentList = argv[1:]

class DatasetDescrption():
	
	def __init__(self):
		
		# DESCRIPTION: initialze class
		
		# if no arguments passed, try to create dataset_description.json in current working directory
		if not argumentList:
			argumentList[0] = os.getcwd()
		
		# create target folder if it does not exist
		if not os.path.isdir(argumentList[0]):
			print('Target folder for dataset_description.json NOT found: %s Making it...' % argumentList[0])
			os.mkdir(argumentList[0])
		
		# input variables
		inputvarList = ['targetfolder', 'Name', 'DatasetType']
		self.inputvar = {inputvarList[i]: argumentList[i] for i in range(len(argumentList))}
	
	def create_dataset_description(self):
		
		# DESCRIPTION: create dataset_description json
		
		default_dict = {'Name': 'Insert dataset name', 'DatasetType': 'raw'}
		data = {}
		
		for elem in default_dict.keys():
			if elem in self.inputvar.keys():
				data[elem] = ''.join([val for key, val in self.inputvar.items() if key is elem])
			else:
				data[elem] = default_dict[elem]
		
		data['BIDSVersion'] = 'v1.6.0'
		data['Authors'] = ['Author 1', 'Author 2']
		
		self.json_string = json.dumps(data, indent=2)
	
	def write_dataset_description(self):
		
		# DESCRIPTION: write dataset_description json to file
		
		self.json_filename = '/'.join([self.inputvar['targetfolder'], 'dataset_description.json'])
		jsonFile = open(self.json_filename, 'w')
		jsonFile.write(self.json_string, indent=2)
		jsonFile.close()
	
	def generate_dataset_description(self):
		
		self.create_dataset_description()
		self.write_dataset_description()
		print('%s created!' % self.json_filename)

if __name__ == '__main__':
	
	ddobj = DatasetDescrption()
	ddobj.generate_dataset_description()
	
	
		
