import os.path
import urllib2 as u2
from BeautifulSoup import BeautifulSoup
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
import types
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
def parseGMT( filename ):
    """
    Parses a gmt file and returns a list of dicts
    """
    gmt_list = []
    with open(filename, 'r') as gmt:
        for line in gmt:
            parsed = line.strip().split('\t')
            pw_id = parsed[0]
            url = parsed[1]
            gene_ids = parsed[2:]
            d = {'src_id': os.path.split(filename)[1], 'pw_id' : pw_id, 'broad_url': url, 
                    'gene_ids':gene_ids}
            gmt_list.append(d)
    return gmt_list

def addBroadMeta( gmt ):
    #info we want
    interested = ['Standard name', 'Brief description', 
              'Full description or abstract', 'Contributed by', 'External links']
    try:
        soup = BeautifulSoup(u2.urlopen(gmt['broad_url']))
        tr = soup.table.table.findAll('tr')
        for row in tr:
            try:
                key = row.th.next  
                if key in interested:
                    if key != 'External links':
                        value = row.td.next
                    else:
                        #links
                        value = [] 
                        for links in row.td.findAll('a'):
                            value.append(links.next)
                    gmt[key] = value
            except AttributeError:
                pass
                #just an empty cell

    except AttributeError:
        #catastrophic error (timeout maybe, badURL, schema changed, ?: just return what we have)
        return gmt

def addToTable(table, gmt ):
    gmt_copy = {}
    for key, value in gmt.iteritems():
        if isinstance(value, types.ListType):
            gmt_copy[key] = '#LIST:'+'~:~'.join(value)
        else:
            gmt_copy[key] = value
    table.put_item(gmt_copy)

def createTable( tname, primary_key, secondary_key=None ):
    #check that table does not already exist
    table = Table(tname)
    try:
        table.describe()
        print "Table", tname, "already exists."
        return table
    except JSONResponseError as jse:
        if jse.error_code == 'ResourceNotFoundException': 
            sc = [HashKey('src_id')]
            if secondary_key:
                sc.append(RangeKey(secondary_key))
            net_data = Table.create(tname, schema=sc, throughput={'read': 5,
'write': 15,
} )
            return net_data
        else:
            #k, something else (not table not existing) happened
            #propagate
            raise(JSONResponseError(jse))


if __name__ == "__main__":
    gmt_list = parseGMT('/home/sgeadmin/hdproject/data/c2.cp.biocarta.v4.0.symbols.gmt') 
    for gmt in gmt_list:
        addBroadMeta(gmt)

    mytable = createTable('net_info_table', 'src_id', 'pw_id')
    with mytable.batch_write() as batch:
        for gmt in gmt_list:
            addToTable(mytable, gmt)
