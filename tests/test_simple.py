import time
import pickle

from odq import Odq

def test_simple():
    o = Odq()
    @o.task
    def add(a, b):
        return a + b

    # flush all
    o.disque_client.execute_command('DEBUG', 'FLUSHALL')

    # instance execute
    assert add.run(1, 2) == 3

    # load from queue
    jid = add(1, 2)
    queue, jobid, payload = o.disque_client.get_job(['add'])[0]
    assert queue == b'add'
    assert jid == jobid
    funcname, args, kwargs = pickle.loads(payload)
    assert funcname == 'add'
    assert args == (1, 2)
    assert kwargs == {}
    o.disque_client.ack_job(jobid)

    # apply diffrent config
    assert add.with_config(debug=True)(1, 2) == 3


def test_delay():
    o = Odq()
    @o.task(delay=1)
    def add(a, b):
        return a + b

    # flush all
    o.disque_client.execute_command('DEBUG', 'FLUSHALL')

    # get_job should be delayed
    jid = add(3, 4)
    t0 = time.time()
    queue, jobid, payload = o.disque_client.get_job(['add'])[0]
    assert jid == jobid
    t1 = time.time()
    assert 0.97 <= t1 - t0 <= 1.03


if __name__ == '__main__':
    test_simple()
    test_delay()
