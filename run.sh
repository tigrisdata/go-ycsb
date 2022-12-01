#!/usr/bin/env bash

# Entrypoint script to run the workload

if [ -n "$TIGRIS_URL" ]; then
	HASPORT=$(echo "$TIGRIS_URL" | grep ':' | wc -l)
	if [ "$HASPORT" -eq 0 ]; then
		echo "incorrectly formatted TIGRIS_URL $TIGRIS_URL"
		exit 1
	fi
	TIGRIS_HOST=$(echo "$TIGRIS_URL" | cut -d: -f1)
	TIGRIS_PORT=$(echo "$TIGRIS_URL" | cut -d: -f2)
fi 

TEST_DB="${TEST_DB:-ycsb_tigris}"
RECORDCOUNT=${RECORDCOUNT:-5000}
OPERATIONCOUNT=${OPERATIONCOUNT:-1000000000}
READALLFIELDS=${READALLFIELDS:-true}
READPROPORTION=${READPROPORTION:-0.4}
UPDATEPROPORTION=${UPDATEPROPORTION:-0.4}
SCANPROPORTION=${SCANPROPORTION:-0.2}
INSERTPROPORTION=${INSERTPROPORTION:-0}
REQUESTDISTRIBUTION=${REQUESTDISTRIBUTION:-uniform}
LOADTHREADCOUNT=${LOADTHREADCOUNT:-1}
RUNTHREADCOUNT=${RUNTHREADCOUNT:-1}
# Run mode, single for repeating a single benchmark run with the same configuration, threaded to run
# in different thread configurations
RUNMODE=${RUNMODE:-single}
RUNTHREADCONF=${RUNTHREADCONF:-"1 2 4 8 16 32 64"}
RUNTHREADDURATION=${RUNTHREADDURATION:-"1h"}
RUNTHREADSLEEPINTERVAL=${RUNTHREADSLEEPINTERVAL:-30}
DROPANDLOAD=${DROPANDLOAD:-0}
ENGINE=${ENGINE:-"tigris"}
FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-"/mnt/fdb-config-volume/cluster-file"}
FDB_API_VERSION=${FDB_API_VERSION:-710}

WORKLOAD="recordcount=${RECORDCOUNT}
operationcount=${OPERATIONCOUNT}
workload=core

readallfields=${READALLFIELDS}

readproportion=${READPROPORTION}
updateproportion=${UPDATEPROPORTION}
scanproportion=${SCANPROPORTION}
insertproportion=${INSERTPROPORTION}

requestdistribution=${REQUESTDISTRIBUTION}"

function benchmark_tigris() {
	# Check if the client works
	echo "Checking if tigris client is ok"

	${CLI_PATH}tigris ping
	if [ $? -ne 0 ]
	then
		echo "Tigris client has problems, will exit in 30 sec"
		sleep 30
		exit 1
	fi

	echo "Using TIGRIS_HOST ${TIGRIS_HOST}"
	echo "Using TIGRIS_PORT ${TIGRIS_PORT}"
	echo "Using TEST_DB ${TEST_DB}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"

	if [ "${DROPANDLOAD}" -gt 0 ]
	then
		echo "Dropping test database"
		${CLI_PATH}tigris drop database $TEST_DB
		sleep 10
		echo "Loading new database"
			${BIN_PATH}/go-ycsb load tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
	fi

	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
				${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
			echo "Run completed, sleeping before running again"
			sleep 20
		done
	elif [ "x${RUNMODE}" == "xmultiple_threads" ]
	then
		while true
		do
			for th in ${RUNTHREADCONF}
			do
				echo "Running benchmark for ${th} thread(s)"
				timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -P workloads/dynamic -p threadcount=${th}
				sleep ${RUNTHREADSLEEPINTERVAL}
			done
		done
	else
		echo "Invalid value in RUNMODE variable. Choose between single and multiple_threads."
	fi
}

function benchmark_fdb() {
	echo "Using FDB_CLUSTER_FILE ${FDB_CLUSTER_FILE}"
	echo "Using FDB_API_VERSION ${FDB_API_VERSION}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"

	if [ ${DROPANDLOAD} -gt 0 ]
	then
		echo "Loading new database"
		${BIN_PATH}/go-ycsb load foundationdb -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
	fi
	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
				${BIN_PATH}/go-ycsb run foundationdb -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
			echo "Run completed, sleeping before running again"
			sleep 20
		done
	elif [ "x${RUNMODE}" == "xmultiple_threads" ]
	then
		while true
		do
			for th in ${RUNTHREADCONF}
			do
				echo "Running benchmark for ${th} thread(s)"
				timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -P workloads/dynamic -p threadcount=${th}
				sleep ${RUNTHREADSLEEPINTERVAL}
			done
		done
	else
		echo "Invalid value in RUNMODE variable. Choose between single and multiple_threads."
	fi
}

echo "${WORKLOAD}" > workloads/dynamic

echo "Using engine ${ENGINE}"

case ${ENGINE} in
	"tigris")
		benchmark_tigris
	;;
  "foundationdb")
  	benchmark_fdb
  ;;
  *)
  	echo "Unknown engine"
  	exit 1
  	;;
esac

