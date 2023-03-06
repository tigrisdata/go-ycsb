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

KEY_PREFIX=$(echo $RANDOM | md5sum | awk '{print $1}' | cut -c -6)
export KEY_PREFIX

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
FAILURE_RETRY_INTERVAL=${FAILURE_RETRY_INTERVAL:-3600}
FIELDLENGTH=${FIELDLENGTH:-100}
FIELDCOUNT=${FIELDCOUNT:-10}
YCSB_LOGS_ON_STDOUT=${YCSB_LOGS_ON_STDOUT:-0}
# Only in multiple threads mode for the last run, otherwise the log file is not limited in size
YCSB_LOG_FILE=${YCSB_LOG_FILE:-/tmp/ycsb.log}
MAXSCANLENGTH=${MAXSCANLENGTH:-1000}
SCANLENGTHDISTRIBUTION=${SCANLENGTHDISTRIBUTION:-uniform}
TIGRIS_POSTFIX_TESTDB=${TIGRIS_POSTFIX_TESTDB:-0}
TIGRIS_INDEX_FIELDCOUNT=${TIGRIS_INDEX_FIELDCOUNT:-0}
TIGRIS_READ_INDEX=${TIGRIS_READ_INDEX:-false}
TIGRIS_DUPLICATE_PK=${TIGRIS_DUPLICATE_PK:-false}

WORKLOAD="recordcount=${RECORDCOUNT}
operationcount=${OPERATIONCOUNT}
workload=core

readallfields=${READALLFIELDS}

readproportion=${READPROPORTION}
updateproportion=${UPDATEPROPORTION}
scanproportion=${SCANPROPORTION}
insertproportion=${INSERTPROPORTION}

requestdistribution=${REQUESTDISTRIBUTION}
maxscanlength=${MAXSCANLENGTH}
scanlengthdistribution=${SCANLENGTHDISTRIBUTION}
"

function benchmark_tigris() {
	# Check if the client works
	echo "Checking if tigris client is ok"

	${CLI_PATH}tigris ping
	if [ $? -ne 0 ]
	then
		echo "Tigris client has problems, will exit in 30 sec"
		echo "Doing list databases to show you the error"
		${CLI_PATH}tigris list projects
		sleep ${FAILURE_RETRY_INTERVAL}
		exit 1
	fi

	echo "Using TIGRIS_HOST ${TIGRIS_HOST}"
	echo "Using TIGRIS_PORT ${TIGRIS_PORT}"
	echo "Using TEST_DB ${TEST_DB}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"
	echo "Using ${TIGRIS_INDEX_FIELDCOUNT} Field Indexes"

	if [ "${TIGRIS_POSTFIX_TESTDB}" -gt 0 ]
	then
		TEST_DB="${TEST_DB}_${KEY_PREFIX}"
	fi

	if [ "${DROPANDLOAD}" -gt 0 ]
	then
		echo "Dropping test database"
		${CLI_PATH}tigris delete-project $TEST_DB --force
		sleep 10
		${CLI_PATH}tigris create project $TEST_DB
		echo "Loading new database"
			if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
			then
				${BIN_PATH}/go-ycsb load tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
			else
				${BIN_PATH}/go-ycsb load tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT} > ${YCSB_LOG_FILE}
			fi
	fi

	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
				if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
				then
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
				else
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT} > ${YCSB_LOG_FILE}
				fi
			echo "Run completed, sleeping before running again"
			sleep ${RUNTHREADSLEEPINTERVAL}
		done
	elif [ "x${RUNMODE}" == "xmultiple_threads" ]
	then
		while true
		do
			for th in ${RUNTHREADCONF}
			do
				echo "Running benchmark for ${th} thread(s)"
				if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
				then
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${th}
				else
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run tigris -p tigris.host="$TIGRIS_HOST" -p tigris.port="$TIGRIS_PORT" -p tigris.dbname="$TEST_DB" -p fieldcount="${FIELDCOUNT}" -p tigris.indexfieldcount="${TIGRIS_INDEX_FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p tigris.indexread="${TIGRIS_READ_INDEX}" -p tigris.duplicatepk="${TIGRIS_DUPLICATE_PK}" -P workloads/dynamic -p threadcount=${th} > ${YCSB_LOG_FILE}
				fi
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
		if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
		then
			${BIN_PATH}/go-ycsb load foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
		else
			${BIN_PATH}/go-ycsb load foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT} > ${YCSB_LOG_FILE}
		fi
	fi
	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
			if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
			then
				timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
			else
				timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT} > ${YCSB_LOG_FILE}
			fi

			echo "Run completed, sleeping before running again"
			sleep ${RUNTHREADSLEEPINTERVAL}
		done
	elif [ "x${RUNMODE}" == "xmultiple_threads" ]
	then
		while true
		do
			for th in ${RUNTHREADCONF}
			do
				echo "Running benchmark for ${th} thread(s)"
				if [ ${YCSB_LOGS_ON_STDOUT} -ne 0 ]
				then
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${th}
				else
					timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${th} > ${YCSB_LOG_FILE}
				fi
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

