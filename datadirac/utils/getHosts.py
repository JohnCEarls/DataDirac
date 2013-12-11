import re
def getHosts():
    myre = re.compile(r'.+ (node[0-9]{3})')
    hosts = ['master']
    with open('/etc/hosts', 'r') as f:
       for line in f:
        if myre.match(line):
            hosts.append( myre.match(line).group(1) )
    return hosts
if __name__ == "__main__":
    for host in getHosts():
        print host, " slots=999"
