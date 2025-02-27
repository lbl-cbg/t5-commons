from queuemanager import QueueManager

qm = QueueManager('test')
qm.status()

q1 = qm.get_queue('files', resources=['internal'])
q2 = qm.get_queue('files2', resources=['internal'])
q3 = qm.get_queue('files3', resources=['internal'])

for i in range(10):
    q1.add({'hello': 'world', 'messagez': i})
    q3.add({'hello': 'world', 'messagez': i})
    q2.add({'hello': 'world', 'messagez': i})

for i in range(20):
    rec = qm.next(['internal'])
    if rec['queue'] == 'files':
        qm.fail(rec['tid'], 'because I said so')
    else:
        qm.finished(rec['tid'])
    print(rec)
