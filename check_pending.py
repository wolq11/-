import requests
s = requests.Session()
s.post('http://127.0.0.1:5000/api/login', json={'username':'0066','password':'0000'})
r = s.get('http://127.0.0.1:5000/api/agent/insight/report')
data = r.json()
for sec in data.get('data', {}).get('sections', []):
    if sec['title'] == '活跃度排名':
        print(sec['content'])
