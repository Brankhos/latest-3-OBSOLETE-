import os, platform, subprocess, re, sys
def get_processor_name():
    if platform.system() == "Windows":
        return platform.processor()
    elif platform.system() == "Darwin":
        os.environ['PATH'] = os.environ['PATH'] + os.pathsep + '/usr/sbin'
        command ="sysctl -n machdep.cpu.brand_string"
        return subprocess.check_output(command).strip()
    elif platform.system() == "Linux":
        command = "cat /proc/cpuinfo"
        all_info = subprocess.check_output(command, shell=True).strip()
        for line in all_info.split("\n"):
            if "model name" in line:
                return re.sub( ".*model name.*:", "", line,1)
    return ""
    
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
loca = os.path.join(os.path.dirname(__file__))

if "Intell" in get_processor_name():
    file1 = open(loca+'\mrequ-i.txt', 'r')
    Lines = file1.readlines()
    for line in Lines:
        install("{}".format(line.strip()))
else:
    file1 = open(loca+'\mrequ-a.txt', 'r')
    Lines = file1.readlines()
    for line in Lines:
        install("{}".format(line.strip()))


import main
nain.main()