#pip install kafka-python pandas plotly
import kafkamain
import datetime
moscow_tz = datetime.timezone(datetime.timedelta(hours=3))

time_of_start_offset = int(datetime.datetime(2023, 5, 26, 0, 7, 0, tzinfo=moscow_tz).timestamp() * 1000)
time_of_end_offset =int(datetime.datetime(2023, 5, 26, 14, 30, 0, tzinfo=moscow_tz).timestamp() * 1000)
topic_old_name = 'lotusTransformedContracts'
topic_last_name = 'amosHeliOrdersTransformed'
start_offset = 12832445
end_offset = 12832800

"""
kafkamain.list_of_topics() - получить список топиков
kafkamain.search_topic_messages_by_time() - поиск сообщений в топиках по времени
kafkamain.search_topic_messages_by_offset() - поиск сообщений в топиках по offset
kafkamain.compare_messages_in_topics() - какие сообщения топика topic_old_name не содержатся в топике topic_last_name
kafkamain.send_message_to_topic() - отправка сообщений
kafkamain.topic_plot() - график сообщений топика
"""
#kafkamain.list_of_topics()

# amosHeliSequentialData_dict = kafkamain.search_topic_messages_by_time(topic_old_name, time_of_start_offset, time_of_end_offset)
# print(len(amosHeliSequentialData_dict), '\n', amosHeliSequentialData_dict)
#
# amosHeliSequentialData_by_offset_dict = kafkamain.search_topic_messages_by_offset(topic_old_name, start_offset, end_offset)
# print(len(amosHeliSequentialData_by_offset_dict), '\n', amosHeliSequentialData_by_offset_dict)
#
# not_found_messages_of_amosHeliSequentialData, found_messages_of_amosHeliSequentialData = kafkamain.compare_messages_in_topics(topic_old_name,topic_last_name, time_of_start_offset)
# print(len(not_found_messages_of_amosHeliSequentialData), not_found_messages_of_amosHeliSequentialData)
#
#
# message = kafkamain.search_topic_messages_by_time(topic_old_name, time_of_start_offset, time_of_end_offset)
# kafkamain.send_message_to_topic('TestTopic', list(message.values()))
#
# kafkamain.topic_plot(topic_old_name, time_of_start_offset)
