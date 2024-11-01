#!/usr/bin/env bash

: <<!
Name: check_permissions.sh
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2024-11-01 13:44:53

Description: 检查权限
- 有管理员权限时可以直接执行后续命令
- 没有管理员权限时检查是否安装了sudo
- 安装了sudo 则使用 sudo 执行，没有则报错退出
!

####################################################################
#+++++++++++++++++++++++++ Define Function ++++++++++++++++++++++++#
####################################################################
#------------------------- Feature Function
function error() {
  echo "ERROR $*"
  exit 1
}

function available() {
  command -v "$1" >/dev/null 2>&1
}

SUDO=
if [ "$(id -u)" -ne 0 ]; then
  if ! available sudo; then
    error "This script requires superuser permissions. Please re-run as root."
  fi

  SUDO="sudo"
fi

####################################################################
#++++++++++++++++++++++++++++++ Main ++++++++++++++++++++++++++++++#
####################################################################
$SUDO fdisk -l
