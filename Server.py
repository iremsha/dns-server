import pickle
import socket
from datetime import timedelta, datetime
from dnslib import DNSError, DNSRecord


class Packet:
    def __init__(self, rr, create_time):
        self.resource_record = rr  # ресурсная запись
        self.create_time = create_time  # время создания записи


#  загркзка кэша из файла
def load_history():
    try:
        with open('data.pickle', 'rb') as f:
            database = pickle.load(f)
        print('history loaded')
    except:
        print('history not exist')
        return {}
    return database


#  дампинг кеша в файл
def save_history(data):
    try:
        with open('data.pickle', 'wb') as f:
            pickle.dump(data, f)
        print('dumping successful')
    except:
        print('dumping error')


#  проверка записи, истёк ttl или нет
def is_expired(packet):
    return datetime.now() - packet.create_time > timedelta(seconds=packet.resource_record.ttl)


#  очистка просроченного кеша
def clear_outdated_cash():
    cache_delta = 0
    for key, value in database.items():
        old_length = len(value)
        database[key] = set(packet for packet in value if not is_expired(packet))
        cache_delta += old_length - len(database[key])
    if cache_delta > 0:
        print(str(datetime.now()) + " - cleared " + str(cache_delta) + " resource records")


#  добавление/обновление записи в кеше
def add_record(rr, date_time):
    k = (str(rr.rname).lower(), rr.rtype)
    if k in database:
        database[k].add(Packet(rr, date_time))
    else:
        database[k] = {Packet(rr, date_time)}


#  извлечение ресурсных записей с ответом, записей с указанием на уполномоченный сервер, записей с дополнительной информацией
def add_records(dns_record):
    for r in dns_record.rr + dns_record.auth + dns_record.ar:
        date_time = datetime.now()
        add_record(r, date_time)
        print(str(date_time) + " - DNS record added.")


#  получение ответа из кеша, если он там есть
def get_response(dns_record):
    key = (str(dns_record.q.qname).lower(), dns_record.q.qtype)
    if key in database and database[key]:
        reply = dns_record.reply()
        reply.rr = [p.resource_record for p in database[key]]
        return reply


#  отправить результат юзеру
def send_response(response, addr):
    sock.connect(addr)
    sock.sendall(response)
    sock.close()


#  основной цикл
def work_loop():
    global sock

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 40000))

    try:
        while True:
            data, addr = sock.recvfrom(2048)

            if database:
                clear_outdated_cash()

            try:
                dns_record = DNSRecord.parse(data)
            except DNSError:
                print('getting incorrect query, packet ignored')
                continue

            add_records(dns_record)
            if not dns_record.header.qr:
                response = get_response(dns_record)
                try:
                    if response:
                        send_response(response.pack(), addr)
                    else:
                        resp = dns_record.send("dns.yandex.ru")
                        add_records(DNSRecord.parse(resp))
                        send_response(resp, addr)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.bind(("", 40000))
                    print(str(datetime.now()) + " - response send")
                except (OSError, DNSError):
                    print(str(datetime.now()) + " - transmission error")
    except:
        print('server error')


def main():
    print('server started')

    global database

    database = load_history()

    try:
        work_loop()
    finally:
        if database:
            save_history(database)
        print('server stop')


if __name__ == '__main__':
    main()
