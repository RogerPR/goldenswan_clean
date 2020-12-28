################################################################################
# Use this script to launch goldenswan scripts (no need to add goldenswan 
#  to PYTHONPATH previously)
################################################################################


import sys
import yaml
import os
import importlib


with  open(os.path.join(os.path.dirname(__file__), "../config.yml"), "r")  as file:
    cfg = yaml.safe_load(file)

# Add goldenswan to PYTHONPATH
sys.path.append(cfg["root"])

# Run script
script_reader = open(sys.argv[1])
script = script_reader.read()
sys.argv = sys.argv[1:]
exec(script)

script_reader.close()

