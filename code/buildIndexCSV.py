"""
20191217: ED - Routine to create an index from the current state of the repository.

Index.csv is a comma-separated file with entries (file, timestamp, relativePath)

This was created because of a limitation in github, which can only return the first 1000 files in the
directory listing of any folder. The current workaround is to create an index to the repository in the ./index folder.

"""
#=========================================================================================
# Last code modification:
#=========================================================================================
__modifiedBy__ = "$modifiedBy: Ed Brooksbank $"
__dateModified__ = "$dateModified: 2019-12-17 13:46 $"
__version__ = "$Revision: 1.0 $"
#=========================================================================================
# Created:
#=========================================================================================
__author__ = "$Author: Ed Brooksbank $"
__date__ = "$Date: 2019-12-17 09:30 $"
#=========================================================================================
# Start of code
#=========================================================================================

import os
import re
from datetime import datetime
import pandas as pd


DATAPATH = 'data'
INDEXPATH = 'index'
OUTFILE = 'index.csv'
ROOT = os.getcwd()
ROOTDATA = os.path.join(ROOT, DATAPATH)
DATAPREFIX = 'ccpnRef_'
DATASEARCH = r'{}(\w.*)_'.format(DATAPREFIX)


if __name__ == '__main__':

    depth = len(ROOT.split(os.path.sep))
    pathDict = {}

    # iterate through the data folder
    for (root, dirs, files) in os.walk(ROOTDATA, topdown=True):

        # get the path relative to the top of the repository
        # (assumes working directory is repository root)
        relativePath = os.path.sep.join(root.split(os.path.sep)[depth:])

        # iterates through the files in this folder
        for fp in files:

            # onl process .xml files
            fBase, ext = os.path.splitext(fp)
            if ext == '.xml':

                # build a dictionary of the required files
                if fp not in pathDict:
                    dateStr = re.findall(DATASEARCH, fBase)

                    # some timestamps do not have milliseconds so ignore in exported table
                    if not dateStr or not(6 <= len(dateStr[0].split('-')) <= 7):
                        raise TypeError('Error, file {} has malformed date (must be dd-mm-yy-H-M-S(optional -mS)'.format(fp))

                    yy, mo, dd, hh, mn, ss = dateStr[0].split('-')[0:6]
                    tm = datetime(int(yy), int(mo), int(dd), int(hh), int(mn), int(ss), 0)

                    # add new item to the dictionary
                    pathDict[fp] = (fp, tm.timestamp(), relativePath)
                else:
                    raise TypeError('Error, file {} already exists in database'.format(fp))

    # convert to pandas and write pandas.csv

    outFile = os.path.join(ROOT, INDEXPATH, OUTFILE)
    csvFile = sorted([fpTuple for fpTuple in pathDict.values()])

    # make a dataframe
    df = pd.DataFrame(csvFile, columns=['file', 'timestamp', 'path'])

    # write the file into the index path of the repository
    df.to_csv(outFile, index=False)

    # # example search for files in dataframe - remember forward slash for regex
    # found = df[df['file'].str.match('.NA\+0')]
