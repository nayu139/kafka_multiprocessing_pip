#!/usr/tools/env python
# -*- coding: UTF-8 -*-

from multiprocessing import Pipe, Process, cpu_count
from kafka import KafkaConsumer
import os

# 可创建子进程核数（子进程数）
PROCESS_NUM = cpu_count() - 1

KAFAKA_HOST = "0.0.0.0"
KAFAKA_PORT = 9092
KAFAKA_TOPIC = "test"
consumer = KafkaConsumer(KAFAKA_TOPIC,
                         bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                             kafka_host=KAFAKA_HOST,
                             kafka_port=KAFAKA_PORT)
                         , auto_offset_reset='earliest'
                         )


def on_line(pipe):
    '''
    此处替代在线检测模块
    '''
    _out_pipe, _in_pipe = pipe

    # 关闭fork过来的输入端
    _in_pipe.close()
    while True:
        try:
            msg = _out_pipe.recv()
            print("Parent ID:  %s" % os.getppid())
            print("Process ID： %s Msg:  %s" % (os.getpid(), msg))
        except EOFError:
            # 当out_pipe接受不到输出的时候且输入被关闭的时候，会抛出EORFError，可以捕获并且退出子进程
            break


def distribute_pipe(in_pipe):
    """
    根据采集数据ID与cpu剩余核数取余，分配数据给不同的进程
    """
    for msg in consumer:
        msg = str(msg.value, encoding='utf-8')
        msg = eval(msg)
        id = msg['itemId']
        i = id % PROCESS_NUM
        in_pipe[i].send(msg)


def main():
    # 初始化管道
    out_pipe = [None] * PROCESS_NUM
    in_pipe = [None] * PROCESS_NUM
    son_p_data = [None] * PROCESS_NUM

    # 数据处理进程
    for i in range(PROCESS_NUM):
        out_pipe[i], in_pipe[i] = Pipe(True)
        son_p_data[i] = Process(target=on_line, args=((out_pipe[i], in_pipe[i]),))

    for i in range(PROCESS_NUM):
        son_p_data[i].start()
        # 等pipe被fork 后，关闭主进程的输出端
        # 这样，创建的Pipe一端连接着主进程的输入，一端连接着子进程的输出口
        out_pipe[i].close()

    distribute_pipe(in_pipe)

    for i in range(PROCESS_NUM):
        son_p_data[i].join()


if __name__ == "__main__":
    main()
