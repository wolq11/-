#!/bin/bash

# 社团管理系统启停脚本

APP_NAME="stuclub"
APP_DIR="/opt/liqi/-"
VENV_PYTHON="$APP_DIR/venv/bin/python"
GUNICORN="$APP_DIR/venv/bin/gunicorn"
WORKERS=4
BIND="127.0.0.1:5000"
PID_FILE="/run/$APP_NAME.pid"
LOG_FILE="$APP_DIR/logs/gunicorn.log"

start() {
    if pgrep -f "gunicorn.*server:app" > /dev/null; then
        echo "$APP_NAME 已在运行"
        return 1
    fi

    mkdir -p "$APP_DIR/logs"

    echo "启动 $APP_NAME..."
    cd "$APP_DIR"
    source "$APP_DIR/venv/bin/activate"

    $GUNICORN --workers $WORKERS \
               --bind $BIND \
               --daemon \
               --pid $PID_FILE \
               --log-file $LOG_FILE \
               server:app

    if [ $? -eq 0 ]; then
        echo "$APP_NAME 启动成功 (PID: $(cat $PID_FILE))"
    else
        echo "$APP_NAME 启动失败"
        return 1
    fi
}

stop() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat $PID_FILE)
        if kill -0 $PID 2>/dev/null; then
            echo "停止 $APP_NAME (PID: $PID)..."
            kill $PID
            sleep 2
            rm -f $PID_FILE
            echo "$APP_NAME 已停止"
        else
            echo "进程不存在，清理PID文件"
            rm -f $PID_FILE
        fi
    else
        # 尝试直接kill
        pkill -f "gunicorn.*server:app" && echo "$APP_NAME 已停止" || echo "$APP_NAME 未运行"
    fi
}

restart() {
    stop
    sleep 1
    start
}

status() {
    if pgrep -f "gunicorn.*server:app" > /dev/null; then
        echo "$APP_NAME 运行中"
        pgrep -f "gunicorn.*server:app" | xargs ps -p | tail -n +2
    else
        echo "$APP_NAME 未运行"
    fi
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0
