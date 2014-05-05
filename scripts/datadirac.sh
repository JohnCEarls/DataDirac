#!/bin/sh
 
### BEGIN INIT INFO
# Provides:          myservice
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Put a short description of the service here
# Description:       Put a long description of the service here
### END INIT INFO
 
# Change the next 3 lines to suit where you install your script and what you want to call it
DIR=/home/sgeadmin/.local/bin
DAEMON=/usr/bin/mpiexec
CONFIG_OPTS="--hostfile /home/sgeadmin/mpi.hosts /home/sgeadmin/.local/bin/datadirac"
DAEMON_NAME=datadiracserver

# This next line determines what user the script runs as.
# Root generally not recommended but necessary if you are using the Raspberry Pi GPIO from Python.
DAEMON_USER=sgeadmin
 
# The process ID of the script when it runs is stored here:
PIDFILE=$DIR/$DAEMON_NAME.pid

. /lib/lsb/init-functions
 
do_start () {
    log_daemon_msg "Starting system $DAEMON_NAME daemon"
    start-stop-daemon  --start --background --pidfile $PIDFILE --make-pidfile --user $DAEMON_USER --startas $DAEMON -- $CONFIG_OPTS
    log_end_msg $?
}
#**********************************
# for debug
#start-stop-daemon  --start --verbose --pidfile $PIDFILE --make-pidfile --user $DAEMON_USER --startas $DAEMON -- $CONFIG_OPTS #***DEBUGGING***
#start-stop-daemon  --start --background --pidfile $PIDFILE --make-pidfile --user $DAEMON_USER --startas $DAEMON -- $CONFIG_OPTS
#************************************
do_stop () {
    log_daemon_msg "Stopping system $DAEMON_NAME daemon"
    start-stop-daemon --stop --pidfile $PIDFILE --retry 20
    log_end_msg $?
}

check_stat () {
   status="0"
   pidofproc "$DAEMON" >/dev/null || status="$?"
   if [ "$status" = 0 ]; then
     log_success_msg "$DAEMON_NAME is running"
     exit 0
   else
     log_failure_msg "$DAEMON_NAME is not running"
     exit $status
   fi
}
 
case "$1" in
 
    start|stop)
        do_${1}
        ;;
 
    restart|reload|force-reload)
        do_stop
        do_start
        ;;
 
    status)
        check_stat
        ;;
    *)
        echo "Usage: /etc/init.d/$DEAMON_NAME {start|stop|restart|status}"
        exit 1
        ;;
 
esac
exit 0
