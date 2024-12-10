#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: q_and_a.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2024-12-10 10:58:01

Description: 调查问卷
"""


def are_you_sure(prompt: str = "Are you sure? [yes/No]: ",
                 default: str = 'no'):
    """用户交互：确认执行"""
    while True:
        response = input(prompt).strip().lower()
        if not response and default:
            return default == 'yes'
        elif response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
            continue


def give_your_choice(options: list,
                     prompt: str = "Select one or more options: "):
    """用户交互：选项列表"""
    # 首先打印所有选项及其对应的编号
    for index, option in enumerate(options, start=1):
        print(f"{index}: {option}")

    while True:
        try:
            # 获取用户输入并去除多余空格，分割成单独的选择
            choices = input(prompt).strip().split(',')
            # 将字符串形式的选择转换为整数，并检查是否在有效范围内
            selected_indices = [
                int(choice.strip()) for choice in choices if choice.strip()
            ]

            # 检查是否有无效的索引
            if not all(1 <= index <= len(options)
                       for index in selected_indices):
                raise ValueError(
                    "Invalid selection. Please enter valid option numbers.")

            # 根据选择的索引获取对应的选项
            selected_options = [
                options[index - 1] for index in selected_indices
            ]
            return selected_options

        except ValueError as e:
            print(e)
            continue
