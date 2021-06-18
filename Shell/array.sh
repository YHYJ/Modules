#!/usr/bin/env bash

: << !
Name: array.sh
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-08-09 22:44:13

Description: 操作数组

Attentions:
-

Depends:
-
!

####################################################################
#+++++++++++++++++++++++++ Define Variable ++++++++++++++++++++++++#
####################################################################
#------------------------- Parameter Variable
readonly arrays=(A B C)

####################################################################
#++++++++++++++++++++++++++++++ Main ++++++++++++++++++++++++++++++#
####################################################################
# 数组长度
echo "${#arrays[@]}"

# 遍历数组并获取下标和对应元素
for array in "${!arrays[@]}"; do
  echo "$array: ${arrays[$array]}"
done
