# CodingChallenges_redis
# Commands to RUN
.\venv\Scripts\activate -> to activate venv
python .\src\main\mini_redis.py -> to start the mini server
<br>docker run --name redis-server -p 6379:6379 -d redis -> to start redis-server tools on docker
<br>docker run --rm redis redis-benchmark -t set,get -n 100000 -q -h host.docker.internal -p 6379 -> to run benchmark
<br>docker run -it --rm redis redis-cli -h host.docker.internal -p 6379 -> to use redis-cli