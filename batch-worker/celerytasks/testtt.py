import datetime
from django.utils import timezone

a = 'localhost,127.0.0.1,10.38.124.82,10.38.65.203'
b = a.split(',')

c = ['10.38.65.203', 'localhost', '127.0.0.1', 'web']

print(f'----------{type(b)}')
print(f'----------{type(c)}')

