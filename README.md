# test

git clone https://github.com/liujian3/test.git

cd test

docker run -v /root/test:/home -it python:3.9 bash

cd /home

pip install requests
