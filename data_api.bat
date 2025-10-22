@echo off
REM =======================================
REM 程序功能：用于window启动python程序的脚本
REM 定时开关：Win + R键，输入taskschd.msc，统计股市数据情况
REM =======================================
cd E:\py-workspace\stock
C:\Users\Administrator\miniforge3\envs\py310\python.exe data_api.py
pause