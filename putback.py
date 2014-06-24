import boto
sdb = boto.connect_sdb('xxx', 'zzz')
recs = sdb.get_domain('recommendations')

f = open('test.txt', 'r')
int = 0
for line in f:
	str = line
	user = str.split(' ')[0][1:]
	friend = str.split(' ')[1][1:]
	score = str.split(' ')[2]
	item_name = int
	item_attrs = {'user': user, 'friend': friend, 'score': score}
	recs.put_attributes(item_name, item_attrs)
	int += 1
