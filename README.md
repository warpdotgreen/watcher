# warp.green Watcher
[warp.green](https://warp.green) cross-chain messaging protocol watcher API (that means no frontend).

## Install & Run

Create virtual environment:
```bash
python3 -m venv venv
. ./venv/bin/activate
```

Install dependencies:
```bash
pip install --extra-index-url https://pypi.chia.net/simple/ chia-blockchain==2.2.0
pip install python-dotenv==1.0.1
pip install web3==6.17.2
pip install fastapi==0.110.2
pip install hypercorn==0.16.0
```

Run watcher:
```bash
python3 watcher.py
```

Run API:
```bash
python3 api.py
```
