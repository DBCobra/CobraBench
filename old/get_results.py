import glob, os, subprocess

os.chdir('results')
print("percent thread throughput ")
for fname in glob.glob("*.txt"):
	# print(fname)
	name = fname.split('.')[0]
	[percent, thread] = name.split('-')
	output = subprocess.check_output("cat %s | grep Throughput; echo ' ERROR ERROR ERROR ERROR';" % fname, shell=True)
	print ("%s %s %s" % (percent.replace('p', ''), thread.replace('t', ''), output.split(' ')[1]))
