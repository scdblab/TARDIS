#!/bin/bash

result_dir="/proj/BG/haoyu/results-tardis"
exe_dir="/proj/BG/haoyu/tardis/emulab"

for try in {0..4}
do
	rm -rf $result_dir
	bash $exe_dir/run_exp.sh
	tar -czvf /proj/BG/haoyu/tardis-$try.tar $result_dir
	rm -rf $result_dir
done