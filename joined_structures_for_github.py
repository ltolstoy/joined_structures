#!/home/ltolstoy/anaconda3/bin/python
"""
Script to read all, current and old, structure_xxx.xml files for all sites, and grab from there sn, mac, location, etc
And save it into separate file, for future processing into db

"""

#import glob,
import fnmatch, os, sys  
import xml.etree.ElementTree as ET
import pickle
import pprint


def ser2mac(ser):
    #converts serial number (most frequently available in the structure.xml) to Mac (available only after struct file is uploaded and commissioned)
    serial= str(ser)
    serial = serial.upper()
    week = int(serial[:2])
    year = int(serial[2:4])
    letter = ord(serial[4]) - 65
    ser = int(serial[5:])

    prefix = '%06X' % ((week << 18) | (year << 11) | (letter << 6))
    suffix = '%06X' % ser

    return prefix + suffix

all = {}    #dict with all the data, use mac as a key

matches = []        #list of all found xml files recursively in the folder
c=1     #count found files
for froot, dirnames, filenames in os.walk('/mnt/data_log/'):
    for filename in fnmatch.filter(filenames, 'structure_*.xml'):
        file = os.path.join(froot, filename)  # full path to current xml
        matches.append(file)   #Now we have the list of all xml files
        #for file in matches: #look into all subfolders recursively
        matches.sort()
        print("{} -- Processing {}".format(c , file))
        block = filename[filename.find('_b') + 1:filename.find('.')]  # Now it can be anything  between "_b" and ".":
        # b1,b2,b3 or b4, or b301_2...b508
        site1 = file.split('/')[4]      # to get 'aikawa' from /mnt/data_log/aikawa/'
        site = site1 + '_' + block      #to get 'aikawa_b1'
        c += 1
        with open(file,'rt') as f:
            try:
                tree = ET.parse(f)
                root = tree.getroot()
                ch = root.attrib['ch']
                gw = root.attrib['gw_addr']
                ed = root.attrib['ed_addr']
                locations = []  # here store string names aka locations
                for m in root.iter('String'):
                    a = m.get('name')  # smth like '08.01.01-1'
                    #b = block + a[2:]  # was    to get '308.01.01-1'
                    locations.append(''.join(a))
                    cnt = 0  # to count corresponding location position in the list
                for m in root.iter('Converter'):
                    #mac = m.get('mac')
                    sn = m.get('sn')
                    mac = ser2mac(sn)   #instead of looking it in struct file, re-create it from sn, as it's more available
                    sku = m.get('sku')
                    ts = m.get('ts')
                    loc = locations[cnt]
                    cnt += 1  # goto next location
                    if mac not in all:
                        all[mac] = [(sn, sku, site, loc, ts, [ch, gw, ed], filename)]
                    else:
                        all[mac].append((sn, sku, site, loc, ts, [ch, gw, ed], filename))
                        
            except:
                print("Something wrong with file {}".format(file))
                print(sys.exc_info())

all_clean = {}      #here instead of lists for each file, keep only  unique data

#for curiosity and control - check if site names and locations are looking good
site_all = []
loc_all = []

for key in all:
    #print key , all[key]
    sn = []
    sku = []
    site = []
    loc = []
    ts = []
    ch_gw_ed = []
    gw = []
    ed = []
    fnam = []
    for i in range( len(all[key]) ): #many lists for each key, according to number of files where this mac was found
        if all[key][i][0] not in sn:
            sn.append(all[key][i][0])      #append sn to set of all sn
        if all[key][i][1] not in sku:
            sku.append(all[key][i][1])     # append next elem to set of corresponding elements
        if all[key][i][2] not in site:
            site.append(all[key][i][2])

        if all[key][i][2] not in site_all:
            site_all.append(all[key][i][2])        # for verification, create list of all sites

        if all[key][i][3] not in loc:
            loc.append(all[key][i][3])
        if all[key][i][3] not in loc_all:       # for verification, append location to list of all locations
            loc_all.append(all[key][i][3])

        if all[key][i][4] not in ts:
            ts.append(all[key][i][4])      #ts

        if all[key][i][5] not in ch_gw_ed:
            ch_gw_ed.append(all[key][i][5])

        #ch.append(all[key][i][5])
        #gw.append(all[key][i][6])
        #ed.append(all[key][i][7])
        if all[key][i][6] not in fnam:
            fnam.append(all[key][i][6])        #was 8 now 6
    all_clean[key] = (list(sn), list(sku),list(site),list(loc),list(ts),list(ch_gw_ed),list(fnam) )

    
#save pickled dictionary
with open('/home/ltolstoy/scripts/joined_structures/all_sites_pickled_v6' ,'wb') as f: #open file in BINARY mode!
    pickle.dump(all_clean,f,protocol=pickle.HIGHEST_PROTOCOL)

pprint.pprint(all_clean)          #nicely output the resulting dict
print("\nTotal unique macs: {}".format(len(all_clean)))
print("Unique sorted sites:")
pprint.pprint(sorted(site_all))
print("\nUnique sorted locations:")
pprint.pprint(sorted(loc_all))
