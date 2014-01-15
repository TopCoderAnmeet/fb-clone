import boto
sdb = boto.connect_sdb('AKIAICX6PH7J362DCMCA', '0VmWENUfO2Z/CwB9l0hCyhBrFcPw6iIneHS+SEnF')
users = sdb.get_domain('users')

query = 'select * from users'
rs = users.select(query)
for j in rs:
	dict = j
	print 'U ' + j.name + ' ' + dict['interests'] + ' ' + dict['network']

query2 = 'select * from friendships'
friendships = sdb.get_domain('friendships')
rs2 = friendships.select(query2)
for j2 in rs2:
	dict = j2
	print 'F ' + dict['f1'] + ' ' + dict['f2']