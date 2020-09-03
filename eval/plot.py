import subprocess
import os

def plot_all():
    DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    subprocess.call('gnuplot ' + DIR_PATH + '/plot/scripts/*.gnuplot', shell=True)

if __name__ == '__main__':
    plot_all()