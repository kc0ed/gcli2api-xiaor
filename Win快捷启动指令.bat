call .venv\Scripts\activate.bat

set REDIS_URI=redis://:74I3ms120Hgh9Ao5lpDKWUZyGEP8OX6R@sjc1.clusters.zeabur.com:24470
set REDIS_DATABASE=6

python web.py
pause