#pip install kafka-python
from kafka import TopicPartition, KafkaConsumer, KafkaProducer
import plotly.graph_objs as go
import pandas as pd
import datetime


conf = {
    'bootstrap_servers': ['99.99.99.99:9999'],
    'max_partition_fetch_bytes': 52428800 * 3,
    'group_id': None,
    'value_deserializer': lambda x: x.decode('utf-8'),
    'max_poll_records': 10000,
    'sasl_plain_username': 'username',
    'sasl_plain_password': 'password'
}

prod_conf = {
    'bootstrap_servers': ['99.99.99.99:9999'],
    'sasl_plain_username': 'username',
    'sasl_plain_password': 'password'
}

def search_topic_messages_by_time(topic_old_name, time_of_start_offset, time_of_end_offset=None ):
    """
    Метод поиска сообщений в топиках по времени
    :param topic_old_name: имя топика
    :param time_of_start_offset: timestamp время начального оффсета
    :param time_of_end_offset: timestamp время конечного оффсета. Если не задано, будет выполнен поиск от time_of_start_offset до последнего оффсета
    :return: возвращает словарь, dict - {offset:value}
    """
    consumer = KafkaConsumer(
        topic_old_name,
        **conf
    )
    topic_partition = TopicPartition(topic_old_name, 0)
    offsetsForTimes = consumer.offsets_for_times({topic_partition: time_of_start_offset})
    for tp, offset_and_timestamp in offsetsForTimes.items():
        start_offset = offset_and_timestamp.offset
        print(f"Топик: {tp[0]}, Партиция: {tp[1]}, StartOffset: {start_offset}")
        consumer.seek(topic_partition, start_offset)
        if not time_of_end_offset:
            endOffsets = consumer.end_offsets([topic_partition])
            for offset_and_timestamp in endOffsets.items():
                end_offset = offset_and_timestamp[1]-1
                print(f"Топик: {tp[0]}, Партиция: {tp[1]}, EndOffset: {end_offset}")
                dict = {}
                current_offset = 0
                while current_offset < end_offset:
                    records = consumer.poll(timeout_ms=3000)
                    if not records:
                        continue
                    for tp, messages in records.items():
                        for message in messages:
                            dict[message.offset] = message.value
                            current_offset = message.offset
            return dict
        else:
            offsetsForTimes2 = consumer.offsets_for_times({topic_partition: time_of_end_offset})
            for tp, offset_and_timestamp in offsetsForTimes2.items():
                end_offset = offset_and_timestamp.offset
                print(f"Топик: {tp[0]}, Партиция: {tp[1]}, EndOffset: {end_offset}")
                dict = {}
                current_offset = 0
                while True:
                    records = consumer.poll(timeout_ms=3000)
                    if not records:
                        continue
                    for tp, messages in records.items():
                        for message in messages:
                            dict[message.offset] = message.value
                            current_offset = message.offset
                            if current_offset >= end_offset:
                                break
                    if current_offset >= end_offset:
                        break
            return dict

def search_topic_messages_by_offset(topic_old_name, start_offset, end_offset=None ):
    """
    Метод поиска сообщений в топиках по offset
    :param topic_old_name: имя топика
    :param start_offset: offset начального сообщения
    :param end_offset: offset конечного сообщения. Если не задано, будет выполнен поиск от start_offset до последнего оффсета
    :return: возвращает словарь, dict - {offset:value}
    """
    consumer = KafkaConsumer(
        **conf
    )
    topic_partition = TopicPartition(topic_old_name, 0)
    consumer.assign([topic_partition])
    consumer.seek(topic_partition, start_offset)
    if not end_offset:
        endOffsets = consumer.end_offsets([topic_partition])
        for offset_and_timestamp in endOffsets.items():
            end_offset = offset_and_timestamp[1]-1
            print(f"Топик: {topic_old_name}, StartOffset: {start_offset}")
            print(f"Топик: {topic_old_name}, EndOffset: {end_offset}")
            dict = {}
            current_offset = 0
            while current_offset < end_offset:
                records = consumer.poll(timeout_ms=3000)
                if not records:
                     continue
                for tp, messages in records.items():
                    for message in messages:
                        dict[message.offset] = message.value
                        current_offset = message.offset
        return dict
    else:
        end_offset = end_offset
        print(f"Топик: {topic_old_name}, StartOffset: {start_offset}")
        print(f"Топик: {topic_old_name}, EndOffset: {end_offset}")
        dict = {}
        current_offset = 0
        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                continue
            for tp, messages in records.items():
                for message in messages:
                    dict[message.offset] = message.value
                    current_offset = message.offset
                    if current_offset >= end_offset:
                        break
            if current_offset >= end_offset:
                break
        return dict

def compare_messages_in_topics(topic_old_name, topic_last_name,time_of_start_offset):
    """
    Метод находит какие сообщения топика 1 не содержатся в топике 2 за указанный временной промежуток
    :param topic_old_name: Имя первого топика
    :param topic_last_name: Имя второго топика
    :param time_of_start_offset: timestamp время начала интересующего периода
    :return: возвращает 2 словаря: not_found - сообщения топика 1 которые были не найдены в топике 2 и found - сообщения, которые были найдены
    """
    topic_old = search_topic_messages_by_time(topic_old_name, time_of_start_offset)
    topic_old_messages = list(topic_old.values())
    topic_last = search_topic_messages_by_time(topic_last_name, time_of_start_offset)
    topic_last_messages = list(topic_last.values())
    not_found = []

    for item in topic_old_messages:
        if item not in topic_last_messages:
            not_found.append(item)

    found = [item for item in topic_last_messages if item not in not_found]

    print(f"В топике {topic_last_name} найдено: {len(found)} сообщений из топика {topic_old_name}")
    print(f"В топике {topic_last_name} не найдено: {len(not_found)} сообщений из топика {topic_old_name}")
    return not_found, found

def send_message_to_topic(old_topic_name, message):
    """
    Отправка сообщений в топик
    :param old_topic_name: имя топика
    :param message: список (list) c сообщениями
    :return:
    """
    if not isinstance(message, list):
        raise ValueError('Message must be a list')
    # создаем экземпляр KafkaProducer
    producer = KafkaProducer(**prod_conf)
    num = 0
    # отправляем несколько сообщений в топик
    for i in message:
        producer.send(old_topic_name, i.encode('utf-8'))
        num += 1
        print(f"Отправлено {num} сообщений из {len(message)} в топик {old_topic_name}")
    # закрываем соединение с Kafka
    producer.close()

def search_for_plot(topic_old_name, time_of_start_offset, time_of_end_offset=None ):
    """
    Метод поиска сообщений для графика
    :param topic_old_name: имя топика
    :param time_of_start_offset: timestamp время начального оффсета
    :param time_of_end_offset: timestamp время конечного оффсета. Если не задано, будет выполнен поиск от time_of_start_offset до последнего оффсета
    :return: возвращает 2 словаря, dict - {offset:value} и timestamp_dict {offset:timestamp}
    """
    consumer = KafkaConsumer(
        topic_old_name,
        **conf
    )
    topic_partition = TopicPartition(topic_old_name, 0)
    offsetsForTimes = consumer.offsets_for_times({topic_partition: time_of_start_offset})
    for tp, offset_and_timestamp in offsetsForTimes.items():
        start_offset = offset_and_timestamp.offset
        print(f"Топик: {tp[0]}, Партиция: {tp[1]}, StartOffset: {start_offset}")
        consumer.seek(topic_partition, start_offset)
        if not time_of_end_offset:
            endOffsets = consumer.end_offsets([topic_partition])
            for offset_and_timestamp in endOffsets.items():
                end_offset = offset_and_timestamp[1]-1
                print(f"Топик: {tp[0]}, Партиция: {tp[1]}, EndOffset: {end_offset}")
                dict = {}
                timestamp_dict = {}
                current_offset = 0
                while current_offset < end_offset:
                    records = consumer.poll(timeout_ms=3000)
                    if not records:
                        continue
                    for tp, messages in records.items():
                        for message in messages:
                            dict[message.offset] = message.value
                            timestamp_dict[message.offset] = message.timestamp
                            current_offset = message.offset
            return dict, timestamp_dict
        else:
            offsetsForTimes2 = consumer.offsets_for_times({topic_partition: time_of_end_offset})
            for tp, offset_and_timestamp in offsetsForTimes2.items():
                end_offset = offset_and_timestamp.offset
                print(f"Топик: {tp[0]}, Партиция: {tp[1]}, EndOffset: {end_offset}")
                dict = {}
                timestamp_dict = {}
                current_offset = 0
                while True:
                    records = consumer.poll(timeout_ms=3000)
                    if not records:
                        continue
                    for tp, messages in records.items():
                        for message in messages:
                            dict[message.offset] = message.value
                            timestamp_dict[message.offset] = message.timestamp
                            current_offset = message.offset
                            if current_offset >= end_offset:
                                break
                    if current_offset >= end_offset:
                        break
            return dict, timestamp_dict


def topic_plot(topic_old_name, time_of_start_offset, time_of_end_offset=None):
    """
    Метод выводящий график появления сообщений в топике
    :param topic_old_name: имя топика
    :param time_of_start_offset: timestamp время начального оффсета
    :param time_of_end_offset: timestamp время конечного оффсета. Если не задано, будет выполнен поиск от time_of_start_offset до последнего оффсета
    :return: гистограмма
    """
    d, tsd = search_for_plot(topic_old_name, time_of_start_offset, time_of_end_offset)

    datetimes_utc0 = [datetime.datetime.fromtimestamp(ts / 1000) for ts in tsd.values()]
    df = pd.DataFrame({'timestamp': datetimes_utc0, 'message_value': list(d.values())})

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d-%m-%y %H-%M').dt.strftime('%d-%m-%y %H-%M')


    # создаем объект go.Histogram
    histogram = go.Histogram(x=df['timestamp'], y=df['message_value'], xbins=dict(size='second'))

    # задаем параметры графика
    layout = go.Layout(title=f"Топик: {topic_old_name}. Всего {len(df['message_value'])} сообщений за период [{df['timestamp'].min()}] - [{df['timestamp'].max()}] (UTC+3)",
                       xaxis=dict(title='DateTime'), yaxis=dict(title='Messages'))
    fig = go.Figure(data=[histogram], layout=layout)

    # выводим гистограмму
    fig.write_html('first_figure.html', auto_open=True)

def list_of_topics():
    """
    Получить список топиков
    :return: Сет названий топиков
    """
    consumer = KafkaConsumer(**conf)
    topics_list = consumer.topics()
    print(topics_list)
    return topics_list
