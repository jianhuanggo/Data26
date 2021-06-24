for n in $1
do
    python /Users/jianhuang/PycharmProjects/Scoot/Daemon/ScootDaemon.py -y worker -i $n start
done
