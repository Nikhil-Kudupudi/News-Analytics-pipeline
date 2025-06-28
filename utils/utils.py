import re

def formatName(filename):
    name=re.sub("[^a-zA-Z0-9]","_",filename)
    return name