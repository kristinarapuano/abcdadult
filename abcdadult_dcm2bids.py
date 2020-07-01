from os.path import join, basename
from subprocess import call
import json
import glob
import pandas as pd
from datetime import datetime
import sys

base_dir = '/gpfs/milgram/project/casey/ABCDadult/'
dat_dir = join(base_dir, 'data')
bids_dir = join(dat_dir, 'BIDS_unprocessed')

config_fn = join(base_dir, 'resources', 'abcdadult_dcm2bids.conf')

def run_dcm2bids(sub=None, session=None):
    '''
    run dcm2bids
    sub     : e.g., NDARABCD_ses1
    session : e.g., ses-01
    '''

    ndar = str.split(sub, '_')[0]

    cmd = f"dcm2bids -d {join(dat_dir, 'DCMs', sub)} \
            -p {ndar} \
            -s {session} \
            -c {config_fn} \
            -o {bids_dir} \
            --forceDcm2niix --clobber"

    print(f'running {cmd}')
    call(cmd, shell=True)

def add_intendedfor(sub=None, session=None):
    '''
    adds intended for field to func jsons
    searches for corresponding field maps occuring closest in time
    sub     : e.g., NDARABCD
    session : e.g., ses-01

    '''

    ndar = str.split(sub, '_')[0]

    fmap_fns = glob.glob(join(bids_dir, ndar, session, 'fmap', f'*func*.json'))
    func_fns =  glob.glob(join(bids_dir, ndar, session, 'func', '*.json'))

    df = pd.DataFrame(index=func_fns, columns=['time', 'fmap_AP', 'fmap_PA'])
    for func_fn in func_fns:
        j = json.load(open(func_fn))
        df.loc[func_fn, 'time'] = j['AcquisitionTime']

    df = df.sort_values(by='time')  # sort by time, this works bc time is already 24H

    for func_fn in df.index:

        # setting an arbitrary date to ensure only times are subtracted
        func_time = datetime.strptime('2019-06-01 ' + df.loc[func_fn, 'time'], '%Y-%m-%d %H:%M:%S.%f')

        for direction in ['AP', 'PA']:

            diff = 500 # starting with an overestimate
            for fmap_fn in fmap_fns:

                if direction not in fmap_fn:
                    continue

                fmap_time = json.load(open(fmap_fn))['AcquisitionTime']
                fmap_time = datetime.strptime('2019-06-01 ' + fmap_time, '%Y-%m-%d %H:%M:%S.%f')
                diff_tmp = func_time - fmap_time
                if diff_tmp.seconds < diff:
                    selected_fmap = fmap_fn
                    diff = diff_tmp.seconds
                else:
                    pass

            df.loc[func_fn, f'fmap_{direction}'] = selected_fmap[-27:-9] # for readability

            json_dat = json.load(open(selected_fmap))
            prev_if = json_dat.get('IntendedFor', [])

            nii_pair = str.split(basename(func_fn), '.json')[0] + '.nii.gz'
            nii_pair = join(session, 'func', nii_pair)

            new_if = prev_if.append(nii_pair)
            new_if = set(prev_if)
            json_dat['IntendedFor'] = list(new_if)

            with open(selected_fmap, 'w', encoding='UTF-8') as j:
                json.dump(json_dat, j, indent=2)

    assert(df['fmap_PA'].str.split('run', expand=True)[1].values == df['fmap_AP'].str.split('run', expand=True)[1].values).all()
    return df
