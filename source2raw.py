#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 11:49:05 2022

@author: patrick
"""

import sys
import os
import pandas as pd
import re
from pathlib import Path
import json

# Input from command line
allargs = sys.argv

class Source2Raw():
	
	def __init__(self):
		
		argumentList = allargs[1:]
		if not len(argumentList) == 4:
			sys.exit('Four required inputs (in order): raw_id, project_id, cimbi_id, mr_id; %s inputs received' % str(len(argumentList)))
		
		# input variables
		inputvarList = ['raw_id', 'project_id','cimbi_id','mr_id']
		self.inputvar = {inputvarList[i]: argumentList[i] for i in range(len(argumentList))}
		self.inputcmd = ' '.join(allargs)
		
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
		
		self.func_dictionary = {'faces': ['faces'], 'reward': ['reward'], 'rest': ['rest', 'resting']}
		self.anat_dictionary = {'T1': ['t1'], 'T2': ['t2']}
		self.fmap_dictionary = {'GRE_FIELD_MAPPING': ['gre_field_mapping']}
		self.raw_file_types = ['T1', 'T2', 'EP2D', 'GRE']
		self.raw_file_notallowed = ['ND']
		
		self.dcmfolders = {}
		
	def process_dcmfolders(self):
		# DESCRIPTION: Process newly convert images for assignment to data_type folder or removal
		
		if len(self.dcmfolders)>0:
			self.sourcefile = {}
			for i in self.dcmfolders:
				print('Processing %s...' % i)
				tmpstr = '^(' + i + ').*(.json)$'
				json_list = [elem for elem in os.listdir(self.bidsinfo['sesfolder']) if re.search(tmpstr, elem)]
				for elem in json_list:
					self.sourcefile[elem] = {}
					self.sourcefile[elem]['oldjson'] = str(Path(self.bidsinfo['sesfolder'], elem))
					self.sourcefile[elem]['oldnii'] = str(Path(self.bidsinfo['sesfolder'], os.path.splitext(elem)[0] + '.nii.gz')) 
					for j in self.bids_data_types:
						matches = [k for k in getattr(self, j + '_dictionary').keys() if [v for v in getattr(self, j + '_dictionary')[k] if re.search(v, elem.lower())]]
						if matches:
							print('%s assigned to %s' % (elem,j))
							self.sourcefile[elem]['data_type'] = j
							if j == 'func':
								self.sourcefile[elem]['task'] = ''.join([k for k in self.func_dictionary.keys() if [v for v in self.func_dictionary[k] if re.search(v, elem.lower())]])
								self.sourcefile[elem]['tail'] = 'bold'
							elif j == 'anat':
								self.sourcefile[elem]['tail'] = ''.join(matches) + 'w'
							break
					
					json_fullpath = Path(self.bidsinfo['sesfolder'], elem)
					f = open(json_fullpath)
					json_data = json.load(f)
					f.close()
					
					if 'AcquisitionTime' in json_data.keys():
						self.sourcefile[elem]['AcquisitionTime'] = json_data['AcquisitionTime']
					else:
						sys.exit('AcquisitionTime key not identified in %s' % elem)
						
					if 'ImageType' in json_data.keys():
						self.sourcefile[elem]['ImageType'] = json_data['ImageType']
					else:
						sys.exit('ImageType key not identified in %s' % elem)
					
					if 'EchoNumber' in json_data.keys():
						self.sourcefile[elem]['EchoNumber'] = json_data['EchoNumber']
					else:
						self.sourcefile[elem]['EchoNumber'] = ''
					
					if self.sourcefile[elem]['data_type'] == 'fmap':
						if 'PHASE' in self.sourcefile[elem]['ImageType']:
							self.sourcefile[elem]['tail'] = 'phasediff'
						elif 'M' and 'NORM' in self.sourcefile[elem]['ImageType']:
							if self.sourcefile[elem]['EchoNumber'] != '':
								self.sourcefile[elem]['tail'] = 'magnitude' + str(self.sourcefile[elem]['EchoNumber'])
							else:
								sys.exit('EchoNumber expected but not found.')
						elif 'M' and not 'NORM' in self.sourcefile[elem]['ImageType']:
							self.sourcefile[elem]['tail'] = ''
		else:
			sys.exit('dcmfolders is empty.')
		
		for elem in self.sourcefile.keys():	
	
			if not self.sourcefile[elem]['tail']:
					continue
			
			if self.sourcefile[elem]['data_type'] == 'func':
				same_data_type = [k for k in self.sourcefile.keys() if self.sourcefile[k]['data_type']==self.sourcefile[elem]['data_type']]
				sorted_acqtimes = sorted([self.sourcefile[k]['AcquisitionTime'] for k in same_data_type if self.sourcefile[k]['task']==self.sourcefile[elem]['task']])	
			
			elif self.sourcefile[elem]['data_type'] == 'fmap' or self.sourcefile[elem]['data_type'] == 'anat':
				same_tail = [k for k in self.sourcefile.keys() if self.sourcefile[k]['tail']==self.sourcefile[elem]['tail']]
				sorted_acqtimes = sorted([self.sourcefile[k]['AcquisitionTime'] for k in same_tail])
			
			curr_run = [idx for idx in range(len(sorted_acqtimes)) if sorted_acqtimes[idx]==self.sourcefile[elem]['AcquisitionTime']]
			if len(curr_run)>1:
				sys.exit('Multiple acquisitions have same time stamp')
			elif len(curr_run)==0:
				sys.exit('No matching acquisition time stamps found')
			else:
				self.sourcefile[elem]['run'] = "%02d" % (curr_run[0]+1,)
			
		
	def move_dcmfolders(self):
		
		# DESCRIPTION: Move dicom files to respective folders
		
		if len(self.sourcefile)>0:
			sub_elem = self.bidsinfo['sub']
			ses_elem = self.bidsinfo['ses']
			for elem in self.sourcefile.keys():
				if not self.sourcefile[elem]['tail']:
					print('Removing %s...' % self.sourcefile[elem]['oldjson'])
					os.remove(self.sourcefile[elem]['oldjson'])
					print('Removing %s...' % self.sourcefile[elem]['oldnii'])
					os.remove(self.sourcefile[elem]['oldnii'])
					continue
				tail_elem = self.sourcefile[elem]['tail']
				data_type_elem = self.sourcefile[elem]['data_type']
				run_elem = '-'.join(['run', self.sourcefile[elem]['run']])
				if self.sourcefile[elem]['data_type'] == 'func':
					task_elem = self.sourcefile[elem]['task']
					newjson = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, run_elem, task_elem, tail_elem]) + '.json'))
					newnii = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, run_elem, task_elem, tail_elem]) + '.nii.gz'))
				elif self.sourcefile[elem]['data_type'] == 'anat':
					newjson = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, tail_elem]) + '.json'))
					newnii = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, tail_elem]) + '.nii.gz'))
				elif self.sourcefile[elem]['data_type'] == 'fmap':
					newjson = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, run_elem, tail_elem]) + '.json'))
					newnii = str(Path(self.bidsinfo['sesfolder'], data_type_elem, '_'.join([sub_elem, ses_elem, run_elem, tail_elem]) + '.nii.gz'))
				
				self.sourcefile[elem]['newjson'] = newjson
				self.sourcefile[elem]['newnii'] = newnii
				print(elem + ':')
				os.rename(self.sourcefile[elem]['oldjson'], self.sourcefile[elem]['newjson'])
				print('FROM: %s' % self.sourcefile[elem]['oldjson'])
				print('TO: %s' % self.sourcefile[elem]['newjson'])
				os.rename(self.sourcefile[elem]['oldnii'], self.sourcefile[elem]['newnii'])
				print('FROM: %s' % self.sourcefile[elem]['oldnii'])
				print('TO: %s' % self.sourcefile[elem]['newnii'])
	
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
	
	def check_mrid(self):
		
		sourceDir = os.listdir(Path(self.mrsource, self.mrscanner))
		mr_matches = [idx for idx in sourceDir if re.search('^' + self.inputvar['mr_id'], idx)]
		if len(mr_matches) == 0:
			sys.exit('No MR IDs in source match input (%s)' % self.inputvar['mr_id'])
		elif len(mr_matches) > 1:
			sys.exit('Specified MR ID ambiguous...%s matches' % len(mr_matches))
		else:
			mr_id_full = ''.join(mr_matches)
			if self.inputvar['mr_id'] != mr_id_full:
				print('Updating mr_id inputvar (%s -> %s)' % (self.inputvar['mr_id'], mr_id_full))
				self.inputvar['mr_id'] = mr_id_full
			else:
				print('mr_id: %s verified!' % self.inputvar['mr_id'])
	
	def run_all(self):
		
		# DESCRIPTION: "all-in-one" function that executes relevant methods in sequence
		self.check_mrid()
		self.check_rawfolder()
		self.check_sesfolder()
		self.check_datafolder()
		self.convert_source_inputs()
		self.process_dcmfolders()
		self.move_dcmfolders()
		
	def convert_source_inputs(self):
		
		# DESCRIPTION: identify source folders to be converted to raw files
		sourceFolder = '/'.join([self.mrsource, self.mrscanner, self.inputvar['mr_id']])
		sourceDir = os.listdir(sourceFolder)
		dcmfolders = [i for i in sourceDir if re.search('^(EP2D|T1|T2|GRE).*([0-9]{4})', i)]
		d2n_path = 'dcm2niix'
		toremove = []
		for i in dcmfolders:
			disallowed_matches = [tmpstr for tmpstr in self.raw_file_notallowed if re.search(tmpstr,i)]
			if disallowed_matches:
				print('Disallowed match found: %s. Skipping dcm2niix...' % i)
				toremove.append(i)
				continue
			imgmatch = [fname for fname in os.listdir(self.bidsinfo['sesfolder']) if re.search('^(' + i + '.*.nii.gz)$', fname)]
			if imgmatch:
				print('Existing match found: %s! Skipping dcm2niix...' % i)
			else:
				print('Converting: %s ' % i)
				dcm2niix_cmd = d2n_path + ' -o ' + self.bidsinfo['sesfolder'] + ' -z y -f ' + i + ' ' + '/'.join([sourceFolder,i])
				os.system(dcm2niix_cmd)
		
		print('Done converting source input folders!')
		self.dcmfolders = [elem for elem in dcmfolders if elem not in toremove]
	
	def generate_participants_file(self):
		
		# DESCRIPTION: generate participants.tsv file
		
		column_set = ['participant_id', 'session_id', 'mr_id', 'pet_id']
		fname = '/'.join([self.bidsinfo['projfolder'], 'participants.tsv'])
		df = pd.DataFrame(columns=column_set)
		df.to_csv(fname, sep = '\t', index = False)
	
if __name__ == '__main__':
	
	self = Source2Raw()
	self.run_all()
	print('Input data: %s %s %s' % (self.inputvar['project_id'], self.inputvar['cimbi_id'], self.inputvar['mr_id']))
	print('Done!')
	
#for i in range(1,3):
#	if i == 1:
#		raw_id = '/mrdata/patrick/raw'
#		project_id = 'np2-p2'
#		cimbi_id = '53888'
#		mr_id = 'p231sc'
#		argumentList = [raw_id, project_id, cimbi_id, mr_id]
#	else:
#		raw_id = '/mrdata/patrick/raw'
#		project_id = 'np2-p2'
#		cimbi_id = '55867'
#		mr_id = 'p240mb'
#		argumentList = [raw_id, project_id, cimbi_id, mr_id]
#	
#	s2r = Source2Raw()
#	s2r.check_rawfolder()
#	s2r.check_sesfolder()
#	s2r.check_datafolder()
#	s2r.convert_source_inputs()
#	s2r.process_dcmfolders()
#	s2r.move_dcmfolders()
	
# python3 /data1/patrick/SCRIPTS/py/source2raw/source2raw.py /mrdata/patrick/raw np2-p2 53888 p231sc 
# python3 /data1/patrick/SCRIPTS/py/source2raw/source2raw.py /mrdata/patrick/raw np2-p2 55867 p240mb
# python3 /data1/patrick/SCRIPTS/py/source2raw/source2raw.py /mrdata/patrick/raw np2-p2 53888 p239sc 
# python3 /data1/patrick/SCRIPTS/py/source2raw/source2raw.py /mrdata/patrick/raw np2-p2 53888 p266

# python3 /mrdata/patrick/source2raw.py /mrdata/patrick/raw np2-p2 53888 p231sc 
