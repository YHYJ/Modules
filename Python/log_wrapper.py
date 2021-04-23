#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: logtool.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-02 15:10:19

Description: 配置logger
"""

import logging
import logging.handlers
import os

LEVEL = 2


def logger_name(level=1, file=__file__):
    """生成logger name

    :level: int     -- 文件所在层级（相对于Project文件夹）
    :returns: str   -- logger name

    """
    # 获取当前文件绝对路径的目录部分
    parent = os.path.dirname(file)
    # 以系统分隔符分隔该部分
    dir_list = parent.split(os.path.sep)

    # level比目录层级数大时取层级数-1（为了屏蔽分隔出的空字符串），小于0时取值1
    if int(level) > len(dir_list) - 1:
        r_level = len(dir_list) - 1
    elif 1 <= int(level) <= len(dir_list) - 1:
        r_level = int(level)
    else:
        r_level = 1

    # 项目和文件夹名
    sep = '.'
    effect_dir = list()
    # 提取有效的文件夹名
    for lv in range(r_level, 0, -1):
        effect_dir.append(dir_list.pop(-lv))
    dirname = sep.join(effect_dir)

    # 文件名
    filename = os.path.splitext(os.path.split(file)[1])[0]

    logger_name = '{prefix}.{postfix}'.format(prefix=dirname, postfix=filename)

    return logger_name


def setup_logging(conf):
    """Initialize the logging module settings

    :conf: initialize parameters
    :return: logger

    """
    level = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }

    console = conf.get('console', False)  # console output?
    console_level = conf.get('console_level', 'DEBUG')  # console log level
    file = conf.get('file', True)  # file output?
    file_level = conf.get('file_level', 'WARNING')  # file log level
    logfile = conf.get('log_file', 'logs/log.log')  # log file save position
    max_size = conf.get('max_size', 10240000)  # size of each log file
    backup_count = conf.get('backup_count', 10)  # count of log files
    log_format = conf.get('format', '%(message)s')  # log format

    logger = logging.getLogger(logger_name(LEVEL))
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')

    if file:
        # 如果 log 文本不存在，创建文本
        dir_path = os.path.dirname(logfile)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # 实例化一个 rotate file 的处理器，让日志文件旋转生成
        fh = logging.handlers.RotatingFileHandler(filename=logfile,
                                                  mode='a',
                                                  maxBytes=max_size,
                                                  backupCount=backup_count,
                                                  encoding='utf-8')
        fh.setLevel(level[file_level])
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if console:
        # 实例化一个流式处理器，将日志输出到终端
        ch = logging.StreamHandler()
        ch.setLevel(level[console_level])
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


if __name__ == "__main__":
    name = logger_name(LEVEL)
    print(name)
