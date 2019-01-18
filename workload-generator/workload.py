import sys


if len(sys.argv) != 2:
    print("Incorrect usage. Example: /{0} <workload file>".format(sys.argv[0]))
    exit()
path = sys.argv[1]
workload_file = None
try:
    workload_file = open(path, 'r')
except:
    print("Workload file not found: {0}".format(path))
    exit()

workload = workload_file.readlines()

for work in workload:
    print(work)

