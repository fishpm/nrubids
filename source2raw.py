#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 11:49:05 2022

@author: patrick
"""

from sys import argv
import os
import pandas as pd
import re
from pathlib import Path

# Input from command line
#argumentList = argv[1:]

raw_id = '/mrdata/patrick/raw'
project_id = 'np2-p2'
cimbi_id = '53888'
mr_id = 'p231sc'
argumentList = [raw_id, project_id, cimbi_id, mr_id]

class Source2Raw():
	
	def __init__(self):
		
		if not len(argumentList) == 4:
			ValueError('Four required inputs (in order): raw_id, project_id, cimbi_id, mr_id; %s inputs received' % str(len(argumentList)))
		
		# input variables
		inputvarList = ['raw_id', 'project_id','cimbi_id','mr_id']
		self.inputvar = {inputvarList[i]: argumentList[i] for i in range(len(argumentList))}
		
		print('Inputs received:')
		for i in range(len(self.inputvar.keys())):
			print('%s: %s' % (list(self.inputvar.items())[i][0], list(self.inputvar.items())[i][1]))
		
		# remove trailing slash for raw_id (if present)
		if self.inputvar['raw_id'][-1] == '/':
			self.inputvar['raw_id'] = self.inputvar['raw_id'][:-1]
		
		# nru specific variables
		self.mrsource = '/rawdata/mr-rh/MRraw'
		self.mrscanner = {'p': 'prisma', 'n': 'mr001', 'm': 'mmr', 'v': 'verio'}[self.inputvar['mr_id'][0]]
		
		# bids elements
		self.bidsinfo = {
				'sourcefolder': '/'.join([self.mrsource, self.mrscanner]),
				'rawfolder': self.inputvar['raw_id'],
				'projfolder': '/'.join([self.inputvar['raw_id'], self.inputvar['project_id']]),
				'sub': 'sub-' + self.inputvar['cimbi_id'],
				'subfolder': '/'.join([self.inputvar['raw_id'], self.inputvar['project_id'], 'sub-' + self.inputvar['cimbi_id']]),
				'dataset_description': '/'.join([self.inputvar['raw_id'], self.inputvar['project_id'], 'dataset_description.json']),
				'participants': '/'.join([self.inputvar['raw_id'], self.inputvar['project_id'], 'participants.tsv'])}
		
		self.bids_data_types = ['anat','func','fmap']
	
	def check_rawfolder(self):
		
		# DESCRIPTION: Check for presence of expected folders/files and generate ones not present but necessary to process current dataset
		
		# check that raw folder exists (make if necessary)
		if not os.path.isdir(self.inputvar['raw_id']):
			print('Raw folder NOT found: %s. Making it...' % self.bidsinfo['rawfolder'])
		else:
			print('Raw folder found: %s' % self.bidsinfo['rawfolder'])
		
		# check that dataset_description.json exists (make if necessary)
		ddfile = '/'.join([self.inputvar['raw_id'], 'dataset_description.json'])
		if not os.path.exists(ddfile):
			print('dataset_description NOT found: %s. Making it...' % self.bidsinfo['dataset_description'])
			os.system('python3 /data1/patrick/SCRIPTS/py/source2raw/CreateDatasetDescription.py ' + self.inputvar['raw_id'])
		else:
			print('dataset_description.json found: %s' % self.bidsinfo['dataset_description'])
		
		# check that project folder exists (make if necessary)
		if not os.path.isdir(self.bidsinfo['projfolder']):
			print('Project folder NOT found: %s. Making it...' % self.bidsinfo['projfolder'])
			os.mkdir(self.bidsinfo['projfolder'])
		else:
			print('Project folder found: %s!' % self.bidsinfo['projfolder'])
		
		# check that participants.tsv file exists (make if necessary)
		if not os.path.exists(self.bidsinfo['participants']):
			print('participants.tsv NOT found: %s. Making it...' % self.bidsinfo['participants'])
			self.generate_participants_file()
		else:
			print('participant.tsv found: %s!' % self.bidsinfo['participants'])
		
		# check that subject folder exists (make if necessary)
		if not os.path.isdir(self.bidsinfo['subfolder']):
			print('Subject folder NOT found: %s. Making it...' % self.bidsinfo['subfolder'])
			os.mkdir(self.bidsinfo['subfolder'])
		else:
			print('Subject folder found: %s' % self.bidsinfo['subfolder'])
	
	def check_sesfolder(self):
		
		# DESCRIPTION: Assign session folder for specific dataset
		
		# check whether and ses-folders in subject folder
		subfolderList = os.listdir(self.bidsinfo['subfolder'])
		subfolderList = [elem for elem in subfolderList if re.search('^(ses-[0-9]{3})$',elem)]
		
		# load participants.tsv for cross-referencing
		dd = pd.read_csv(self.bidsinfo['participants'], sep = '\t')
		print('Reading: %s... ' % self.bidsinfo['participants'])
		
		# flag lines in participants.tsv with participant id that matches input 
		sub_matches = dd.index[dd['participant_id'] == self.bidsinfo['sub']].tolist()
		
		if sub_matches:
			
			# skip matching mr id, implies scan session already added
			# line with matching mr id not found, build new session folder
			if self.inputvar['mr_id'] in dd.iloc[sub_matches]['mr_id'].values:
				
				mr_matches = dd.index[dd['mr_id'] == self.inputvar['mr_id']].tolist()
				
				self.bidsinfo['ses'] = dd.iloc[mr_matches]['session_id'].values[0]
				self.bidsinfo['sesfolder'] = '/'.join([self.bidsinfo['subfolder'], self.bidsinfo['ses']])
				print('Session folder found: %s!' % self.bidsinfo['sesfolder'])
				
			else:
				
				self.bidsinfo['ses'] = 'ses-' + f"{len(sub_matches)+1:03d}"
				self.bidsinfo['sesfolder'] = '/'.join([self.bidsinfo['subfolder'], self.bidsinfo['ses']])
				print('Session folder NOT found: %s. Making it...' % self.bidsinfo['sesfolder'])
				self.update_participants()
				os.mkdir(self.bidsinfo['sesfolder'])
				
		# first instance of participant_id, build ses-001 folder
		else:
			print('%s not assigned' % (self.bidsinfo['sub']))
			self.bidsinfo['ses'] = 'ses-001'
			self.bidsinfo['sesfolder'] = '/'.join([self.bidsinfo['subfolder'], self.bidsinfo['ses']])
			print('Session folder NOT found: %s. Making it...' % self.bidsinfo['sesfolder'])
			self.update_participants()
			os.mkdir(self.bidsinfo['sesfolder'])
		
	def check_datafolder(self):
		
		# DESCRIPTION: check whether data folders need to be generated
		
		sesfolderList = os.listdir(self.bidsinfo['sesfolder'])
		
		for i in self.bids_data_types:
			if i in sesfolderList:
				print('Data folder found: %s' % i)
			else:
				print('Data folder NOT found: %s. Making it...' % i)
				os.mkdir('/'.join([self.bidsinfo['sesfolder'], i]))
		
	def update_participants(self):
		
		# DESCRIPTION: update participants.tsv file
		
		# NOTE: to_csv works if code run in python but from shell (not sure why)
		
		dd = pd.read_csv(self.bidsinfo['participants'], sep = '\t')
		dd = dd.append({'participant_id': self.bidsinfo['sub'],'session_id': self.bidsinfo['ses'], 'mr_id': self.inputvar['mr_id']}, ignore_index = True)
		dd.to_csv(self.bidsinfo['participants'], sep = '\t', index = False)
		print('%s file updated!' % self.bidsinfo['participants'])
	
	def check_all(self):
		
		# DESCRIPTION: "all-in-one" function that executes relevant methods in sequence
		
		self.check_rawfolder()
		self.check_sesfolder()
		self.check_datafolder()
		self.identify_source_inputs()
		
	def identify_source_inputs(self):
		
		# DESCRIPTION: identify source folders to be converted to raw files
		sourceFolder = '/'.join([self.mrsource, self.mrscanner, self.inputvar['mr_id']])
		sourceDir = os.listdir(sourceFolder)
		dcmfolders = [i for i in sourceDir if re.search('^(EP2D|T1|T2|GRE).*([0-9]{4})', i)]
		d2n_path = 'dcm2niix'
		for i in dcmfolders:
			print('Converting: %s ' % i)
			dcm2niix_cmd = d2n_path + ' -o ' + self.bidsinfo['sesfolder'] + ' -z y -f ' + i + ' ' + '/'.join([sourceFolder,i])
			os.system(dcm2niix_cmd)
		print('Done reading source input folders!')
	
	def generate_participants_file(self):
		
		# DESCRIPTION: generate participants.tsv file
		
		column_set = ['participant_id', 'session_id', 'mr_id', 'pet_id']
		fname = '/'.join([self.bidsinfo['projfolder'], 'participants.tsv'])
		df = pd.DataFrame(columns=column_set)
		df.to_csv(fname, sep = '\t', index = False)
	
if __name__ == '__main__':
	
	s2r = Source2Raw()
	s2r.check_all()
	print('Input data: %s %s %s' % (s2r.inputvar['project_id'], s2r.inputvar['cimbi_id'], s2r.inputvar['mr_id']))
		
s2r = Source2Raw()
s2r.check_rawfolder()
s2r.check_sesfolder()
s2r.check_datafolder()
s2r.identify_source_inputs()