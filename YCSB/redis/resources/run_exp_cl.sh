#!/bin/bash
db="mongodb"
dryrun=false
totalthreads="20"
dur=600
suffex="z"
client_ip="h1"
cache_ip="h2"
redis_caches_ports="h2:11211"
db_machine="h0"
dbfail="30,40"

result_dir="/proj/BG/yaz/ycsbcache/scripts/ycsb/results"
ycsb_dir="/tmp/YCSB"
numrecs="100000"

base_dir="/proj/BG/yaz/ycsbcache/scripts"
metricsFile="/tmp/metrics.json"
twemcache_bin="/tmp/IQ-Twemcached/src/twemcache"

cache_size="10000"
warmup="100000"

mkdir -p $result_dir

start_stats() {
    s=$1
    tag=$2
    statsdir=$3
    echo "Start stats for " $tag $s
    ssh -oStrictHostKeyChecking=no $s "killall sar"
    ssh -oStrictHostKeyChecking=no $s "sar -P ALL 10 > $statsdir/$tag$s-cpu.txt &"
    ssh -oStrictHostKeyChecking=no $s "sar -n DEV 10 > $statsdir/$tag$s-net.txt &"
    ssh -oStrictHostKeyChecking=no $s "sar -r 10 > $statsdir/$tag$s-mem.txt &"
    ssh -oStrictHostKeyChecking=no $s "sar -d 10 > $statsdir/$tag$s-disk.txt &"
}

end_stats() {
    s=$1
    ssh -oStrictHostKeyChecking=no $s "killall sar"
}

clean_stats() {
    s=$1
    tag=$2
    statsdir=$3

    for stats in "cpu" "net" "mem" "disk"
    do
        java -jar $base_dir/ExtractSarInfo.jar ${stats^^} $statsdir/$tag$s-$stats.txt $statsdir/clean-$tag$s-$stats.txt
        python $base_dir/admCntrl.py $statsdir/clean-$tag$s-$stats.txt $statsdir/$tag$s-$stats.txt
    done
}

run_exp() {
    wl=$1
    numar=$2
    alpha=$3
    cachemode=$4
    tardismode=$5

    spec="exp-$wl-$totalthreads-threads-tr-$numrecs-ar-$numar-alpha-$alpha-arsleep-$arSleep-$dur-warm-$warmup-$cachemode-$tardismode"
    echo $spec
    expdir="$result_dir/$spec"
    echo $expdir
    
    mkdir -p $expdir

    echo "*******************************************"
    echo "*******************************************"
    echo "*******************************************"
    echo "*******************************************"
    echo "**************** wl=$wl **********************"
    echo "**************** totalthreads=$totalthreads **********************"
    echo "**************** duration=$dur **********************"
    echo "**************** expdir=$expdir **********************"
    echo "**************** config=$config **********************"
    echo "**************** arSleep=$arSleep **********************"
    echo "**************** numar=$numar **********************"
    echo "*******************************************"
    echo "*******************************************"
    echo "*******************************************"
    echo "*******************************************"

    mkdir -p $expdir
    statsdir=$expdir"/stats"
    mkdir -p $statsdir

    # skip reload db

    killall java
    echo "Starting caches...:"
    echo "start cache at " $cache_ip
    ssh -oStrictHostKeyChecking=no $cache_ip "killall twemcache"
    sleep 5
    ssh -n -f -oStrictHostKeyChecking=no $cache_ip screen -L -S cache -dm "$twemcache_bin -m $cache_size -g 1000 -G 999999 -t 8"
    sleep 5
    
    echo "Warmup"
    cmd="java -jar /tmp/YCSB.jar -threads 1 -db com.yahoo.ycsb.db.CADSWbMongoDbClient -P workloads/workloadc -P workloads/db.properties -p recordcount=$numrecs -p stringkey=false -p operationcount=$warmup -p requestdistribution=sequential -s -threads 20 -p mongodb.writeConcern=acknowledged -p redis.hosts=$redis_caches_ports -p mongo.host=$db_machine -p ar=0 -p alpha=10 -p readBW=true -p updateBW=false -p arBW=false -p arSleep=1000 -p metricsFile=$metricsFile -p dbfail=1000,2000 -p cachemode=through -p slaresponsetime=100 2>&1"
    echo "warmup client with command $cmd "
    if [ "$dryrun" == false ]; then
        eval "$cmd"
    fi
    echo "Finished warmup"

    killall java
    sleep 5

    # preparing sar

    echo "Preparing sar"
    start_stats $db_machine "db" $statsdir
    start_stats $cache_ip "cache" $statsdir
    start_stats $client_ip "client" $statsdir
    
    echo "Begin Experiment..."
    cmd="bash $base_dir/run_ycsb.sh $db $wl $numrecs $dur $totalthreads $redis_caches_ports $db_machine $numar $readBW $updateBW $arBW $arSleep $metricsFile $dbfail $alpha $write_back"
    echo "running client with command $cmd "

    if [ "$dryrun" == false ]; then
        ssh -oStrictHostKeyChecking=no $client_ip "rm /tmp/screenlog"
        ssh -oStrictHostKeyChecking=no -n -f $client_ip screen -L -S ycsb -dm "$cmd"
    fi


    if [ $dryrun == false ]; then
        # wait for exp to finish.
        while ssh -oStrictHostKeyChecking=no $client_ip "screen -list | grep -q ycsb"
        do
            ((sleepcount++))
            sleep 60
            echo "waiting for client "
        done
    fi

    echo "Experiment finished"
    echo "killing sar"

    end_stats $db_machine
    end_stats $cache_ip
    end_stats $client_ip


    totalthru="0"
    s=$client_ip
    ssh -oStrictHostKeyChecking=no -n -f $s "cp /tmp/screenlog $expdir/client$s"
    ssh -oStrictHostKeyChecking=no -n -f $s "rm /tmp/screenlog "

    ssh -oStrictHostKeyChecking=no -n -f $s "cp /tmp/metrics.json $expdir/client-$s-metrics.json"
    ssh -oStrictHostKeyChecking=no -n -f $s "rm /tmp/metrics.json"

    sleep 1

    grep "Throughput(ops/sec)" $expdir"/client"$s | cut -f3- -d, >> $expdir"/thru.txt"
    thru=$(grep "Throughput(ops/sec)" $expdir"/client"$s | cut -f3- -d,)
    totalthru=$(echo "$totalthru + $thru" | tr -d $'\r'| bc)

    # # throughput graph
    python $base_dir/parse_throughput.py "$expdir/client$s" "$expdir/client$s-throughput.out"
    python $base_dir/admCntrl.py "$expdir/client$s-throughput.out" "$expdir/client$s-throughput"
    # read latency graph
    python $base_dir/parse_latency.py "$expdir/client$s" "READ:" "$expdir/client$s-read-latency.out"
    python $base_dir/admCntrl.py "$expdir/client$s-read-latency.out" "$expdir/client$s-read-latency"
    # update latency graph
    python $base_dir/parse_latency.py "$expdir/client$s" "UPDATE:" "$expdir/client$s-update-latency.out"
    python $base_dir/admCntrl.py "$expdir/client$s-update-latency.out" "$expdir/client$s-update-latency"

    echo $totalthru >> $expdir"/thru.txt"
    sudo killall sar
    echo "Generating charts..."

    clean_stats $db_machine "db" $statsdir
    clean_stats $cache_ip "cache" $statsdir
    clean_stats $client_ip "client" $statsdir
    killall java
}

dur="600"
dbfail="0,240"
totalthreads="20"
wl="workloada"

warmup="50000"
for alpha in "1" "10" "100"
do
    for numar in "1" "10" "100"
    do
	cachemode="through"

	tardismode="tard"
        run_exp $wl $numar $alpha $cachemode $tardismode
        
	tardismode="dis"
       	run_exp $wl $numar $alpha $cachemode $tardismode

	tardismode="tardis"
        run_exp $wl $numar $alpha $cachemode $tardismode

	cachemode="back"
	run_exp $wl $numar $alpha $cachemode $tardismode
    done
done
