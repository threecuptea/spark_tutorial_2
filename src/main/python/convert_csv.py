import sys
from time import time


_, file_name = sys.argv

input = open(file_name, 'r')

dot_ind = file_name.rfind('.')
csv_file = file_name + '.csv'
if dot_ind > -1:
    csv_file = file_name[:dot_ind] + '.csv'

output = open(csv_file, 'w')

for line in input:
    parts = line.split("::")
    output.write("%s" % ",".join(parts))

output.close()
input.close()




