#!/bin/bash
user="haoyu"
redis_bin="/users/haoyu/tardis/redis-3.2.9/src/redis-server"
db="redis"
dryrun=false
totalthreads="20"
dur=600
suffex="z"
client_ip="h1"
cache_ip="h2"
redis_caches_ports="h2:11211"
db_machine="h0"
dbfail="30,40"

result_dir="/proj/BG/haoyu/results-tardis"
ycsb_dir="/tmp/YCSB"
numrecs="100000"

base_dir="/proj/BG/haoyu/tardis/emulab"
metricsFile="/tmp/metrics.json"

cache_size="10gb"
warmup="100000"

reload_db() {
    echo "reload db with $numrecs Records"
    if [ "$numrecs" == "1000000" ]; then
        bash $base_dir/installmongo1m.sh
    elif [ "$numrecs" == "10000000" ]; then
        bash $base_dir/installmongo10m.sh
    elif [ "$numrecs" == "100000" ]; then
        bash $base_dir/installmongo100k.sh
    fi
}

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
    readBW=$3
    updateBW=${4}
    arBW=${5}
    arSleep=${6}
    exp_prefix=${7}
    config=$8
    alpha=$9
    write_back=${10}
    readAlBW=${11}
    writeSetValue=${12}

    spec="new-paper-$exp_prefix-$config-$wl-$totalthreads-threads-tr-$numrecs-ar-$numar-alpha-$alpha-arsleep-$arSleep-$dur-warm-$warmup-$write_back-exp"
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

    reload_db

    killall java
    echo "Starting caches...:"
    echo "start cache at " $cache_ip
    ssh -oStrictHostKeyChecking=no $cache_ip "killall redis-server"
    sleep 5
    ssh -n -f -oStrictHostKeyChecking=no $cache_ip screen -L -S cache -dm "$redis_bin --port 11211 --maxmemory $cache_size --protected-mode no"
    # if ssh -oStrictHostKeyChecking=no $cache_ip "screen -list | grep -q cache"
    #     echo "cache " " is running"
    # else
    #     echo "cache " " is not running  running #ERROR"
    # fi
    sleep 5
    
    echo "Warmup"
    cmd="cd $ycsb_dir && ./bin/ycsb run $db -P workloads/workloadc -P workloads/db.properties -p recordcount=$numrecs -p stringkey=false -p operationcount=$warmup -p requestdistribution=sequential -s -threads 20 -p mongodb.writeConcern=journal -p redis.hosts=$redis_caches_ports -p mongo.host=$db_machine -p ar=0 -p alpha=10 -p readBW=true -p updateBW=false -p arBW=false -p arSleep=1000 -p metricsFile=$metricsFile -p dbfail=1000,2000 -p writeBack=false -p slaresponsetime=100 -p readAlBW=false -p writeSetValue=false 2>&1"
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
    cmd="bash $base_dir/run_ycsb.sh $db $wl $numrecs $dur $totalthreads $redis_caches_ports $db_machine $numar $readBW $updateBW $arBW $arSleep $metricsFile $dbfail $alpha $write_back $readAlBW $writeSetValue"
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
numar="10"
arSleep="100"
exp_prefix="EXPR"
readBW="true"
updateBW="true"
arBW="true"
config="tardis"
alpha="100"
write_back="false"
readAlBW="false"
writeSetValue="false"

# for alpha in "1" "10" "100" "1000"
# do
#     for numar in "10" "100"
#     do
#         readBW="true"
#         updateBW="true"
#         arBW="true"
#         config="tardis"
#         run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha
#     done
# done

warmup="50000"
for alpha in "1" "10" "100"
do
    for numar in "1" "10" "100"
    do
        readAlBW="false"
        writeSetValue="false"
        write_back="false"
        readBW="true"
        updateBW="true"
        arBW="true"
        config="tardis"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue
        readBW="true"
        updateBW="false"
        arBW="true"
        config="tard"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue
        readBW="false"
        updateBW="true"
        arBW="true"
        config="dis"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue
        write_back="true"
        readBW="true"
        updateBW="true"
        arBW="true"
        config="tar"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue

        write_back="true"
        readBW="true"
        updateBW="false"
        arBW="true"
        config="tardisread"
        readAlBW="true"
        writeSetValue="false"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue

        write_back="true"
        readBW="true"
        updateBW="true"
        arBW="true"
        config="tardiswrite"
        readAlBW="false"
        writeSetValue="true"
        run_exp $wl $numar $readBW $updateBW $arBW $arSleep $exp_prefix $config $alpha $write_back $readAlBW $writeSetValue
    done
done
