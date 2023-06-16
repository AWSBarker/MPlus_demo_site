# helper to get data file ip using CWD/data as root
def load_data(filepath):
    with open(filepath, 'r') as f:
        lastip = f.readlines()[-1]
    return lastip.rstrip()