# launches nmon utility for performance metric collection
# and parses its outputs into several distinct csv files

# use timestamp to create a distinct
# job identifier, if none provided
JOB_ID=${2:-$(date --iso-8601=seconds)}
BASE_FIFO="/tmp/nmon-dump-${JOB_ID}"

# create pipe for communication
NMON_FIFO="${BASE_FIFO}-source"
mkfifo $NMON_FIFO || exit

# run nmon in the background and
# capture its pid, writes will be done
# to the pipe created above
# note: nmon parameters can be parsed from user input
# if required, ones below are hardcoded for now
# note: the FIRST line emitted by nmon into stdout
# would be its PID which can be used to cancel
# its worker process externally (kill -USR2 $NMON_PID)
nmon -F $NMON_FIFO -p -s 1 -c 120 &

# run something else here
# this script should block until the
# work is done
# reading from pipe with cat would block
# until pipe is closed (i.e., nmon has stopped)
cat $NMON_FIFO
# another option is to wait until nmon process terminates
# while kill -0 $NMON_PID 2> /dev/null; do sleep 1; done;

# cleanup
rm $NMON_FIFO && echo 'Removed pipe'
