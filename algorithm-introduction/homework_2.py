#!/usr/bin/env python

import os, tarfile

statistics = dict({'regular_file_num':0,'positive_value':0})

def unpack_tar_gz(path_name):
	if os.path.isfile(path_name):
		if tarfile.is_tarfile(path_name):
			archive = tarfile.open(path_name, 'r:gz')
			for tarinfo in archive:
				archive.extract(tarinfo, os.path.dirname(path_name))
				# recursive invoke
				unpack_tar_gz(os.path.join(os.path.dirname(path_name), tarinfo.name))
		else:
			statistics['regular_file_num'] = statistics.get('regular_file_num') + 1
			with open(path_name) as f:
				for line in f.readlines():
					try:
						if int(line) > 0:
							statistics['positive_value'] = statistics.get('positive_value') + int(line)
					except ValueError as e:
						pass


if __name__ == '__main__':
	tar_file_path = input("Please input tar file path(abs): ")
	if (os.path.isabs(tar_file_path)):
		unpack_tar_gz(tar_file_path)
		print(statistics)
	else:
		print("Input path is not abs path.")
