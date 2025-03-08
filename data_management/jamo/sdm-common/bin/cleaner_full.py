#!/usr/bin/env python2
import h5py
import numpy as np
from pbcore.io import CmpH5Reader
import shutil
import argparse
import re
from Bio import SeqIO

### Script to remove control sequence from data sets
### Author Kjiersten Fagnan, kmfagnan@lbl.gov
### December 2013

# Function that cleans out the holes from the bax.h5 files
def strip_holes(bax_file_name, holes_to_remove):
	# Make a 'clean' copy of the file for editing
	clean_fname = 'clean.'+ re.split('/',bax_file_name.strip())[-1]
	shutil.copy(bax_file_name.strip(),clean_fname)
	# Open the 'clean' file for read/write
	clean_bax1 = h5py.File(clean_fname,'r+')
	# check that the edits are to the start of the region and the end of the region in case the columns change
	# copy the regions table to maintain a "working copy" for future releases or something that at least is 
	# backward compatible.
	rtable_dset = clean_bax1['PulseData/Regions'] 
	rtable = clean_bax1['PulseData/Regions'].value
	# Create backup Region table, so changes to file are self-contained
	clean_bax1.create_dataset('PulseData/Regions.back',rtable.shape, rtable.dtype)
	clean_bax1['PulseData/Regions.back'].write_direct(rtable)
	# Extract the hole numbers that will be used to match the control sequence
	holes = clean_bax1.get('/PulseData/BaseCalls/ZMW/HoleNumber')
	# Only look at the subset of holes that can be present in this bax.h5 file 
        rmv_hole = holes_to_remove[(holes_to_remove <= holes[-1]) & (holes_to_remove >= holes[0]) ]
        # Set the quality value to -1
	index_to_modify = np.in1d(rtable[:,0],rmv_hole)
        
	# If this his has been entered as an include rather than exclude list, negate the booleans in the index array
	if INCLUDE_HOLES:
		index_to_modify =~ index_to_modify
	# Need the column names so we can verify that we're modifying the appropriate column... this feels brittle to me
	colnames = rtable_dset.attrs['ColumnNames']	
	
	# Get index corresponding to the Region score column
	ind = np.where(colnames == "Region score")[0][0]
	# Set the accuracy to -1
	rtable[index_to_modify,ind]=-1
        
	# Set the region lengths to 0
	# Get index corresponding to the Region score column
	ind = np.where(colnames == "Region start in bases")[0][0]
        
	rtable[index_to_modify,ind]=0
	ind = np.where(colnames == "Region end in bases")[0][0]
        
	rtable[index_to_modify,ind]=0
	# Write the data out to the clean h5 file
	clean_bax1['PulseData/Regions'].write_direct(rtable)
	clean_bax1.close()
	del clean_bax1
# end of strip_holes function

def clean_all_files(fname,movieIDs,movieInfo,holes_to_rm,movie_dict):
	holes_to_remove_sub = []
	if ('fasta' in fname):
	# Clean a fasta file using the control_reads_cmph5
		records =[]
		fname_out = re.split('/',fname)[-1]
		
		for seq_record in SeqIO.parse(fname, "fasta"):
			line_decomp = re.split('/',seq_record.id)
			movie_name = line_decomp[0]
			zmw_hole = line_decomp[1]
			
                        if INPUT_CMPH5:
                                for movie_id in set(movieIDs):
                                # Find the movie that this read belongs to
                                        if movieInfo[int(movie_id)-1][1] in movie_name:
                                                holes_to_remove_sub = holes_to_rm[movieIDs == movie_id]
                        else:
                                if len(movie_dict.keys()) > 0:
					holes_to_remove_sub = movie_dict[movieInfo[int(movie_id)-1][1]]
				else:
					holes_to_remove_sub = []
                        # determine if the zmw_hole is in that set, if not, length will be zero and we should write this out
                        # Make sure that these arrays have non-zero length
			if len(holes_to_remove_sub) > 0:
				if len(holes_to_remove_sub[holes_to_remove_sub == int(zmw_hole)]) == 0:
					records.append(seq_record)
		
		SeqIO.write(records,"clean."+fname_out,"fasta")
		records =[]
	elif ('fastq' in fname):
		records = []
		fname_out = re.split('/',fname)[-1]
		for seq_record in SeqIO.parse(fname, "fastq"):
			line_decomp = re.split('/',seq_record.id)
			movie_name = line_decomp[0]
			zmw_hole = line_decomp[1]
                        if INPUT_CMPH5:
                                for movie_id in set(movieIDs):
                                        # Find the movie that this read belongs to
                                        if movieInfo[int(movie_id)-1][1] in movie_name:
                                                holes_to_remove_sub = holes_to_rm[movieIDs == movie_id]
                        else:
				if len(movie_dict.keys()) > 0:
					holes_to_remove_sub = movie_dict[movieInfo[int(movie_id)-1][1]]
				else:
					holes_to_remove_sub = []
                                holes_to_remove_sub = movie_dict[movieInfo[int(movie_id)-1][1]]
			# Make sure that these arrays have non-zero length
			if len(holes_to_remove_sub) > 0:		
				if len(holes_to_remove_sub[holes_to_remove_sub==int(zmw_hole)]) == 0:
					records.append(seq_record)
		SeqIO.write(records,"clean."+fname_out,"fastq")
		records = []
	else: 
		# This must be an .bax.h5 file
		bax_file = fname
		#Loop over all movieIDs found in the control reads, this will allow us to access the correct bax files
		#slightly redundant looping
                
                if INPUT_CMPH5:
                        for movie_id in set(movieIDs):
				if movieInfo[int(movie_id)-1][1] in bax_file.strip():
				# Get the holes to remove for the particular movie and pass into the strip_holes function
                                        holes_to_remove_sub = holes_to_rm[movieIDs == movie_id]
					strip_holes(bax_file.strip(), np.asarray(holes_to_remove_sub))
                else:
                        movie_name = re.split('/', bax_file.strip())[-1]
                        movie_name = re.split('\.', movie_name)[0]
			if len(movie_dict.keys()) > 0:
				if movie_name in movie_dict.keys():
					holes_to_remove_sub = map(int,movie_dict[movie_name])
					strip_holes(bax_file.strip(), np.asarray(holes_to_remove_sub))
                # only strip the holes out if the file matches
		# end loop over movie IDs list
	# end loop over bax file list
#end of clean_all_files function

def get_fast_holes(control_name, type):
	print "not done yet"
	
def extract_holes(control_list):
	#need to set up holes_to_rm that can be masked with the movie IDs
	zmw_list = open(control_list,'r').readlines()
	tmp = [re.split('/',s) for s in zmw_list] 
	# Assumes a one-line header, might fail if there are more header lines, should standardize this
	movie_dict = dict()
	for movie in tmp[1:]:
		if len(movie) < 2:
			print "Fail! Your text file was malformed. "+movie
		else:
			if movie[0]  in movie_dict:
				movie_dict[movie[0]].append(movie[1])
			else:
				movie_dict[movie[0]] = [movie[1]]
        return movie_dict
	
	
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("input_list",help="The file that contains the list of bax.h5 files that need to be cleaned.")
	parser.add_argument("control_file",help="The file that contains the list of controls that need to be removed.")
	parser.add_argument("--include_holes",help="Determines whether the list in the control file is included or excluded.  Exclude is used by default.", action="store_true")
	# specify output file names, needs an array to capture the user input
	#parser.add_argument("--outputs",help="Specify the output file names.", action="store_true")
        global INPUT_CMPH5
        global INCLUDE_HOLES
	movieIDs = []
	movieInfo = []
	holes_to_rm = []
	INPUT_CMPH5 = True
        INCLUDE_HOLES = False
	args = parser.parse_args()

        if args.include_holes:
                INCLUDE_HOLES = True

	# Real script will call for 2 inputs
	inputs_file_name = args.input_list
	control_seq_name = args.control_file

	# Set up the holes that need to be removed
	if 'fasta' in control_seq_name:
		get_fast_holes(control_seq_name,'fasta')
                INPUT_CMPH5 = False
	elif 'fastq' in control_seq_name:
		get_fast_holes(control_seq_name, 'fastq')
                INPUT_CMPH5 = False
	elif 'txt' in control_seq_name:
		movie_holes_dict = extract_holes(control_seq_name)
                INPUT_CMPH5 = False
	else:
		# The controlReads object takes in the control_reads.cmp.h5 and contains the information about the 
		# holes that need to be removed. 
		controlReads = CmpH5Reader(control_seq_name)
		movieInfo = controlReads.movieInfoTable
		# Get the alignmentIndex object out of the CmpH5 object so we can get the movie data
		alnIndex = controlReads.alignmentIndex
		# These are the holes that need to be removed from our other files
		holes_to_rm = alnIndex.HoleNumber
		# Get the movie indices for each hole number - this will allow us to remove the data 
		# from the appropriate bax.h5 files
		movieIDs = alnIndex.MovieID

	#Should be content for the "main" function		
	# Create a loop based on the number of files in the inputs_name file
	if 'fasta' in inputs_file_name: 
		clean_all_files(inputs_file_name.strip(),movieIDs,movieInfo,holes_to_rm,movie_holes_dict)
	elif 'fastq' in inputs_file_name:
		clean_all_files(inputs_file_name.strip(),movieIDs,movieInfo,holes_to_rm,movie_holes_dict)
	elif 'fofn' in inputs_file_name:
		bax_inputs = open(inputs_file_name.strip(), 'r')
		for fname in bax_inputs.readlines():
			clean_all_files(fname.strip(),movieIDs,movieInfo,holes_to_rm,movie_holes_dict)
		# end of loop over multiple files
	# end of cases for file input types
	
if __name__ == "__main__":
	main()

