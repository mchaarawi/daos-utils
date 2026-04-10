import argparse
import datetime
import kafka
import json
import re

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--config", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--metric", required=True)
    parser.add_argument("--latest", action='store_true')
    parser.add_argument("--rank")

    args = parser.parse_args()

    #
    # construct pattern
    #
    pattern = re.compile(
       r'%s\{'
       r'cluster="(?P<cluster>[^"]+)",'
       r'host="(?P<host>[^"]+)"'
       r'(,pool="(?P<pool>[^"]+)")?'
       r'(,rank="(?P<rank>\d+)")?'
       r'(,size="(?P<size>\w+)")?'
       r'(,target="(?P<target>\d+)")?'
       r'\}\s+'
       r'(?P<value>\d+)\s+'
       r'(?P<timestamp>\d+)' % (args.metric)
    )

    #
    # load kafka config info
    #
    f = open(args.config)
    kafkaConfig = json.load(f)
    f.close()

    if args.latest:
        kafkaConfig['auto_offset_reset'] = 'latest'
    else:
        kafkaConfig['auto_offset_reset'] = 'earliest'

    #kafkaConfig['consumer_timeout_ms'] = 1000

    #
    # initialize consumer
    #
    consumer = kafka.KafkaConsumer(args.topic,
                                   group_id='daos-tool',
                                   **kafkaConfig)


    #
    # process messages
    #
    #   print out counter when its value exceeds the last value
    #
    last = {}
    for message in consumer:
        msgstr = message.value.decode('utf-8')
        mo = pattern.search(msgstr)
        if mo:
            host = mo.group('host')
            value = float(mo.group('value'))
            pool = mo.group('pool')
            rank = mo.group('rank')
            size = mo.group('size')
            target = mo.group('target')
            key = "{0}:{1}:{2}:{3}:{4}".format(host,pool,rank,size,target)
            if key not in last or value > last[key]:
                dt = datetime.datetime.fromtimestamp(int(mo.group('timestamp')) / 1000)
                print(host, args.metric, value, pool, rank, target, dt.isoformat())
                last[key] = value

    return

if __name__ == '__main__':
    main()
