#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: checker_judge.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2021-06-23 11:30:38

Description: 数据结构检查模块，给定数据必须符合：
    data = {
        'fields': {
            'XXX': {},
            'YYY': {}
        }
    }
之格式要求，即：
1. 给定数据是一个字典，其中必须嵌套一个key为'fields'的字典
2. 'fields'字典中的所有元素都是字典

或全由符合该格式的字典组成的列表也可
"""

good = {
    'timestamp': '2021-06-23 11:26:57',
    'schema': 'device_monitor',
    'table': 'device_temperature',
    'deviceid': 'sensor_temp_humi_06',
    'fields': {
        'temp': {
            'value': 1
        }
    }
}

bad = {
    'timestamp': '2021-06-23 11:26:57',
    'schema': 'device_monitor',
    'table': 'device_auto_bsr01',
    'deviceid': 'device_auto_bsr01',
    'fields': {
        'addr': 'XXX',
        'error_message': 'XXX',
        'status': {
            'value': 0
        },
    }
}


def checker(data):
    """检查数据结构是否符合要求

    :data: 待检查数据
    :returns: bool

    """
    # 默认检查结果符合要求
    result = 1

    if isinstance(data, dict):
        # 当data是字典，只有其'fields'元素的值是个双层嵌套字典才符合要求
        fields = data.get('fields')
        # 检查'fields'是否非空字典
        if fields and isinstance(fields, dict):
            for field in fields.values():
                # 检查'fields'的第二层是否字典
                if not isinstance(field, dict):
                    result -= 1
        else:
            result -= 1
    elif isinstance(data, list):
        # 当data是列表，只有其每个元素都是字典
        # 且每个字典的'fields'元素的值是个双层嵌套字典才符合要求
        for dat in data:
            # 检查列表的元素是否非空字典
            if dat and isinstance(dat, dict):
                fields = dat.get('fields')
                # 检查'fields'是否非空字典
                if fields and isinstance(fields, dict):
                    for field in fields.values():
                        # 检查'fields'的第二层是否字典
                        if not isinstance(field, dict):
                            result -= 1
                else:
                    result -= 1
            else:
                result -= 1
    else:
        result -= 1

    return result


result = checker(bad)

judge = True if result == 1 else False
print('{}，有{}个元素不符合要求'.format(judge, 1 - result))
