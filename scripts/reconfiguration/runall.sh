mkdir exp
rm -rf out *zip

./tpcc-s.sh
mkdir exp/ts
mv *zip exp/ts/

./tpcc-motiv.sh
mkdir exp/motiv
mv *zip exp/motiv/

./ycsb-l.sh
mkdir exp/ycsbl
mv *zip exp/ycsbl/

./tpcc-100.sh
mkdir exp/t100
mv *zip exp/t100/

./ycsb-l-expand.sh
mkdir exp/ly-expand
mv *zip exp/t100/ly-expand

