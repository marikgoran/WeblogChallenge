from datetime import datetime
from pyspark import SparkConf, SparkContext
import uuid

conf = SparkConf().setMaster("local").setAppName("PayTM")
sc = SparkContext(conf=conf)
SESSION_THRESHOLD = 15*60
INPUT_FILE='sample.log'
#INPUT_FILE='full.log'

def get_data(elem):
    """
    Filter out the fields needed for analysis and convert them to proper type for easier and faster processing
    :param elem: one line of the log
    :return: K-V tuple of the ip address as key and the timestamp and url in a dict as value
    """
    fields = elem.split()
    ip = fields[2].split(':')[0]
    ts = datetime.strptime(fields[0], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
    if '"' in fields[12]:
        url='<invalid url according to rfc3986>'
    else:
        url=fields[12]
    return (ip, {'ts': ts, 'url': url})

def tag_sessions(elem):
    """
    Group all url hits from same client inside a session and calculate the length and uniq hits. It does all the heavy lifting to sessionize the data
    :param elem: The input is the groupByKey tuple of the get_data() function.
    :return: List of sessions. Each session is a dict with uuid and 5 attributes. Being a list, it is best to be called from flatMap, instead of map
    """
    hits = elem[1]
    ip = elem[0]
    sessions = []
    uniqs = set()
    sorted_hits = sorted(hits, key=lambda x: x['ts'])
    start_tstamp = sorted_hits[0]['ts']
    previous_tstamp = start_tstamp

    for hit in sorted_hits:
        tstamp = hit['ts']
        if tstamp - previous_tstamp < SESSION_THRESHOLD:
            # we are in the same session, just roll over to the next hit
            uniqs.add(hit['url'])
        else:
            # pack the previous session and start new session
            sessions.append({'id': str(uuid.uuid4()),
                             'ip': ip,
                             'start': start_tstamp,
                             'end': previous_tstamp,
                             'hits': len(uniqs),
                             'length': previous_tstamp-start_tstamp})
            start_tstamp = tstamp
            uniqs = set()
        # roll over the the next hit
        previous_tstamp = tstamp
    # after the for loop, we have to append the final session
    sessions.append({'id': str(uuid.uuid4()),
                     'ip': ip,
                     'start': start_tstamp,
                     'end': previous_tstamp,
                     'hits': len(uniqs),
                     'length': previous_tstamp-start_tstamp})
    return sessions

logs = sc.textFile(INPUT_FILE)
# 1)
sessions = logs.map(get_data).groupByKey().flatMap(tag_sessions)

# 2)
total_time = sessions.map(lambda item: item['length']).reduce(lambda a,b: a+b)
print ("Global session avg: ", total_time / sessions.count())

# 3) The unique hits per session are already included in 1), here we will just sort them using that value as key

# 4)
engaged_users = sessions.map(lambda item: (item['length'], item)).sortByKey(ascending=False)


## Answer section ##
# limit the answers to 10 entries each, for speed and simplicity

# answer1 = sessions.take(10)
answer4 = engaged_users.collect()
for out in answer4:
   print ("***",out)

