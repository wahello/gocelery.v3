# Copyright (c) 2019 Sick Yoon
# This file is part of gocelery which is released under MIT license.
# See file LICENSE for full license details.

from worker import add, add_reflect, qname
from datetime import timedelta,datetime

ar = add_reflect.apply_async(kwargs={'a': 2, 'b': 3}, serializer='json', expires=120, queue=qname)
print('Result: {}'.format(ar.get()))
ar = add.apply_async(args=[4, 5], serializer='json', expires=120, queue=qname)
print('Result: {}'.format(ar.get()))

