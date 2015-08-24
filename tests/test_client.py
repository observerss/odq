import json
import logging
from odq.client import Client

def test_client():
    logging.basicConfig(level=logging.DEBUG)

    c = Client(['localhost:7712', 'localhost:7711'])
    c.connect()
    job = json.dumps(["hello", "1234"])
    print(c.add_job("test", job))

    jobs = c.get_job(['test'], timeout=5)
    for queue_name, job_id, payload in jobs:
        print(job_id)
        c.ack_job(job_id)

if __name__ == '__main__':
    test_client()
