#!/bin/sh

# 1 = topic
# 2 = thread count
# 3 = classifier count
# 4 = classifier start port

while true; do
echo "STARTING METACRAWLER"
# this will kill rainbows of other metacrawlers too, but they should recover
pkill rainbow
sleep 5
python metacrawler.py -l info -t $2 -c $3 -f $4 $1 2>&1 | tee "metacrawler_$(date +%s).log"
done
