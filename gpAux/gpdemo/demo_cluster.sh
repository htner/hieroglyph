#!/usr/bin/env bash

# ======================================================================
# Configuration Variables
# ======================================================================

# Set to zero to force cluster to be created without data checksums

# ======================================================================
# Data Directories
# ======================================================================

if [ -z "${COORDINATOR_DATADIR}" ]; then
  DATADIRS=${DATADIRS:-`pwd`/datadirs}
else
  DATADIRS="${COORDINATOR_DATADIR}/datadirs"
fi

# ======================================================================
# Functions
# ======================================================================

checkDemoConfig(){
    echo "----------------------------------------------------------------------"
    echo "                   Checking for port availability"
    echo "----------------------------------------------------------------------"
    echo ""
    # Check if Coordinator_DEMO_Port is free
    echo "  Coordinator port check ... : ${COORDINATOR_DEMO_PORT}"
    PORT_FILE="/tmp/.s.PGSQL.${COORDINATOR_DEMO_PORT}"
    if [ -f ${PORT_FILE} -o  -S ${PORT_FILE} ] ; then 
        echo ""
        echo -n " Port ${COORDINATOR_DEMO_PORT} appears to be in use. " 
        echo " This port is needed by the Coordinator Database instance. "
        echo ">>> Edit Makefile to correct the port number (COORDINATOR_PORT). <<<" 
        echo -n " Check to see if the port is free by using : "
        echo " 'ss -an | grep ${COORDINATOR_DEMO_PORT}'"
        echo " If the port is not used please make sure files ${PORT_FILE}* are deleted"
        echo ""
        return 1
    fi
    return 0
}

USAGE(){
    echo ""
    echo " `basename $0` {-c | -d | -u} <-K>"
    echo " -c : Check if demo is possible."
    echo " -d : Delete the demo."
    echo " -K : Create cluster without data checksums."
    echo " -u : Usage, prints this message."
    echo ""
}

#
# Clean up the demo
#

cleanDemo(){

    ##
    ## Attempt to bring down using GPDB cluster instance using gpstop
    ##
    pkill -9 postgres

    cmd="configure tenant_mode=disabled; writemode on; begin; clearrange \"\" \\xFF;  commit;"
    fdbcli --exec "$cmd" || true

    bucket_name="sdb1"
    which mc
    if (( $? != 0)); then
        echo "mc command not found"
        return 2
    fi
    mc alias set demo-cluster/ http://127.0.0.1:9000 minioadmin minioadmin
    mc mb --ignore-existing --region=us-east-1 demo-cluster/${bucket_name}

    ( source ${GPHOME}/greenplum_path.sh;)

    ##
    ## Remove the files and directories created; allow test harnesses
    ## to disable this
    ##

    if [ "${GPDEMO_DESTRUCTIVE_CLEAN}" != "false" ]; then
        if [ -f hostfile ]; then
            echo "Deleting hostfile"
            rm -f hostfile
        fi
        if [ -f clusterConfigFile ]; then
            echo "Deleting clusterConfigFile"
            rm -f clusterConfigFile
        fi
        if [ -f clusterConfigPostgresAddonsFile ]; then
            echo "Deleting clusterConfigPostgresAddonsFile"
            rm -f clusterConfigPostgresAddonsFile
        fi
        if [ -d ${DATADIRS} ]; then
            echo "Deleting ${DATADIRS}"
            rm -rf ${DATADIRS}
        fi
        if [ -d logs ]; then
            rm -rf logs
        fi
        rm -f optimizer-state.log gpdemo-env.sh
    fi
}

#*****************************************************************************
# Main Section
#*****************************************************************************

while getopts ":cdK'?'" opt
do
	case $opt in 
		'?' ) USAGE ;;
        c) checkDemoConfig
           RETVAL=$?
           if [ $RETVAL -ne 0 ]; then
               echo "Checking failed "
               exit 1
           fi
           exit 0
           ;;
        d) cleanDemo
           exit 0
           ;;
        K) DATACHECKSUMS=0
           shift
           ;;
        *) USAGE
           exit 0
           ;;
	esac
done

if [ -z "${GPHOME}" ]; then
    echo "FATAL: The GPHOME environment variable is not set."
    echo ""
    echo "  You can set it by sourcing the greenplum_path.sh"
    echo "  file in your Greenplum installation directory."
    echo ""
    exit 1
fi

cat <<-EOF
	======================================================================
	            ______  _____  ______  _______ _______  _____
	           |  ____ |_____] |     \ |______ |  |  | |     |
	           |_____| |       |_____/ |______ |  |  | |_____|

	----------------------------------------------------------------------

EOF

if [ -d $DATADIRS ]; then
  rm -rf $DATADIRS
fi

#*****************************************************************************************
# Create the cloud service 
#*****************************************************************************************
#

echo ""
echo "create account"
echo "-----------------------------"
sdb_stool createaccount -a sdb -p 123 -o sdb 

echo ""
echo "create db"
echo "-----------------------------"
sdb_stool createdb -o sdb -d template1 

echo ""
echo "create user"
echo "-----------------------------"
sdb_stool createuser -o sdb -u sdb -p 123 

echo ""
echo "create account"
echo "-----------------------------"
sdb_stool createaccount -a test -p 123 -o test

echo ""
echo "create user"
echo "-----------------------------"
sdb_stool createuser -o test -u test -p 123 

mkdir -p log
pkill -9 sdb_proxy
pkill -9 sdb_schedule 
pkill -9 sdb_lake 
pkill -9 consul
pkill -9 sdb_account 

#cmd="configure tenant_mode=disabled; writemode on; begin; clearrange \"\" \\xFF;  commit;"
#fdbcli --exec "$cmd" || true

consul agent -dev > log/consul.log 2>&1 &
sleep 1

#make -C ../Makefile
nohup sdb_proxy > log/proxy.log 2>&1 &
nohup sdb_schedule > log/schedule.log 2>&1 &
nohup sdb_lake > log/lake.log 2>&1 &
nohup sdb_account > log/account.log 2>&1 &

netstat -lnpt |  grep proxy
netstat -lnpt |  grep schedule 
netstat -lnpt |  grep lake 
netstat -lnpt |  grep account 


#*****************************************************************************************
# init template1 
#*****************************************************************************************
#

RETURN=$?
CURRENT_DIR=$(cd `dirname $0`; pwd)
echo "=========================================================================================="
echo "executing:"

echo "$GPHOME/bin/initdb -D ${CURRENT_DIR}/datadirs/initdb0"
echo "=========================================================================================="
echo ""

#

$GPHOME/bin/initdb -D ${CURRENT_DIR}/datadirs/initdb0

cat > gpdemo-env.sh <<-EOF
	## ======================================================================
	##                                gpdemo
	## ----------------------------------------------------------------------
	## timestamp: $( date )
	## ======================================================================

EOF

if [ "${RETURN}" -gt 1 ];
then
    # gpinitsystem will return warnings as exit code 1
    exit ${RETURN}
else
    exit 0
fi
