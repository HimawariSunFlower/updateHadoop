@echo off
setlocal enabledelayedexpansion

echo ================================
echo Go应用Linux可执行文件打包脚本
echo ================================

REM 设置变量
set PROJECT_NAME=updateHadoop
set BINARY_NAME=updateHadoop-linux

REM 检查Go是否安装
echo 检查Go环境...
go version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: 未找到Go环境，请先安装Go
    pause
    exit /b 1
)

REM 清理之前的构建文件
echo 清理之前的构建文件...
if exist %BINARY_NAME% del %BINARY_NAME%
if exist %BINARY_NAME%.tar.gz del %BINARY_NAME%.tar.gz

REM 获取目标平台架构
echo 请选择目标平台架构:
echo 1. Linux AMD64 (x86_64)
echo 2. Linux ARM64 (aarch64)
echo 3. Linux 386 (32-bit)
set /p choice="请输入选项 (1-3): "

REM 根据选择设置环境变量
if "%choice%"=="1" (
    set GOOS=linux
    set GOARCH=amd64
    set TARGET_DESC=Linux AMD64
) else if "%choice%"=="2" (
    set GOOS=linux
    set GOARCH=arm64
    set TARGET_DESC=Linux ARM64
) else if "%choice%"=="3" (
    set GOOS=linux
    set GOARCH=386
    set TARGET_DESC=Linux 386
) else (
    echo 无效选项
    pause
    exit /b 1
)

REM 设置环境变量并编译
echo.
echo 正在为 %TARGET_DESC% 编译...
echo GOOS=%GOOS% GOARCH=%GOARCH%
set GOOS=%GOOS%
set GOARCH=%GOARCH%

REM 构建应用
echo 开始构建...
go build -o %BINARY_NAME% .

if %errorlevel% neq 0 (
    echo.
    echo 错误: 构建失败
    pause
    exit /b 1
)

REM 检查构建结果
if not exist %BINARY_NAME% (
    echo.
    echo 错误: 构建文件未生成
    pause
    exit /b 1
)

REM 显示构建信息
echo.
echo 构建成功!
dir %BINARY_NAME%
echo.

REM 询问是否创建压缩包
set /p compress="是否创建tar.gz压缩包? (y/n): "
if /i "%compress%"=="y" (
    echo 创建压缩包...
    tar -czf %BINARY_NAME%.tar.gz %BINARY_NAME%
    if !errorlevel! equ 0 (
        echo 压缩包创建成功: %BINARY_NAME%.tar.gz
        dir %BINARY_NAME%.tar.gz
    ) else (
        echo 警告: 压缩包创建失败
    )
)

echo.
echo 打包完成!
echo 可执行文件: %BINARY_NAME%
if exist %BINARY_NAME%.tar.gz (
    echo 压缩包: %BINARY_NAME%.tar.gz
)
echo.

pause
