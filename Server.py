import pickle
import socket
from datetime import timedelta, datetime
from dnslib import DNSError, DNSRecord

forward_server = "ns1.e1.ru"


class Packet:
    def __init__(self, rr, create_time):
        self.resource_record = rr
        self.create_time = create_time


def load_cache():
    try:
        with open('data.pickle', 'rb') as f:
            database = pickle.load(f)
        print('history cache')
    except:
        print('cache not exist')
        return {}
    return database


def save_cache(data):
    try:
        with open('data.pickle', 'wb') as f:
            pickle.dump(data, f)
        print('save cache done')
    except:
        print('save cache error')


def check_cache(packet):
    return datetime.now() - packet.create_time > timedelta(seconds=packet.resource_record.ttl)


def clear_old_cash():
    cache_delta = 0
    for key, value in database.items():
        old_length = len(value)
        database[key] = set(packet for packet in value if not check_cache(packet))
        cache_delta += old_length - len(database[key])
    if cache_delta > 0:
        print(str(datetime.now()) + " - cleared " + str(cache_delta) + " resource records")


def add_record(rr, date_time):
    k = (str(rr.rname).lower(), rr.rtype)
    if k in database:
        database[k].add(Packet(rr, date_time))
    else:
        database[k] = {Packet(rr, date_time)}


def add_records(dns_record):
    for r in dns_record.rr + dns_record.auth + dns_record.ar:
        print(r)
        date_time = datetime.now()
        add_record(r, date_time)


def get_response_from_cache(dns_record):
    print("get cache answer")
    key = (str(dns_record.q.qname).lower(), dns_record.q.qtype)
    if key in database and database[key]:
        reply = dns_record.reply()
        reply.rr = [p.resource_record for p in database[key]]
        return reply


def send_response(response, addr):
    sock.connect(addr)
    sock.sendall(response)
    sock.close()


def work_loop():
    global sock

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 53))

    try:
        while True:
            data, addr = sock.recvfrom(2048)

            if database:
                clear_old_cash()

            try:
                dns_record = DNSRecord.parse(data)
                # print(dns_record)
            except DNSError:
                print('error parse')
                continue

            add_records(dns_record)
            if not dns_record.header.qr:
                response = get_response_from_cache(dns_record)
                try:
                    if response:
                        print(response)
                        send_response(response.pack(), addr)
                        if database:
                            save_cache(database)
                    else:
                        resp = dns_record.send(forward_server)
                        add_records(DNSRecord.parse(resp))
                        send_response(resp, addr)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.bind(("", 53))
                    if database:
                        save_cache(database)
                except (OSError, DNSError):

                    print("can not ask server " + forward_server + " time: " + str(datetime.now()))
    except:
        print('server error')


def main():
    print('server started')

    global database

    database = load_cache()

    try:
        work_loop()
    finally:
        if database:
            save_cache(database)
        print('server stop')


if __name__ == '__main__':
    main()