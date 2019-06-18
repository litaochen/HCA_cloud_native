import csv
import xml.etree.ElementTree as ET
import sys
import os

# only accept one file
if len(sys.argv) != 2:
    print("Got more than one files. Please only provide one file for processing")
    exit(1)

# check if file exists
if not os.path.isfile(sys.argv[1]):
    print("file does not exist: " + sys.argv[1])
    exit(1)

# set common variables
xml_doc = sys.argv[1]
output_file = os.path.splitext(os.path.basename(xml_doc))[0] + '.csv'

print("reading xml document...")
tree = ET.parse(xml_doc)
root = tree.getroot()

# a json object store the grouping information
groups = {}

# extract the information we are interested in
for image in root.iter('Image'):
    filename = image.attrib['filename']
    the_well = image.find('Well')
    well = the_well.get('label')
    row = the_well.find('Row').get('number')
    column = the_well.find('Column').get('number')
    field = image.find('Identifier').get('field_index')
    color = image.find('EmissionFilter').get('name')
    URL_title = "URL_" + color
    group_id = well + "@" + field

    # update the group list
    if not group_id in groups:
        groups[group_id] = {
            "Row_Number": row,
            "Column_Number": column,
            "Field_Index": field,
            URL_title: filename
        }
    else:
        groups[group_id][URL_title] = filename

# write result to csv file
print("writing data to csv file: " + output_file)
rows = []
for key, val in groups.items():
    rows.append(val)

with open(output_file, 'w') as f:
    w = csv.DictWriter(f, rows[0].keys())
    w.writeheader()
    w.writerows(rows)
