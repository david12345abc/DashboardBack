# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, r"C:\Users\testii\Downloads\dash\DashboardBack")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Dashbord.settings")
import django

django.setup()
from comdir.common import connect_ctx

with connect_ctx() as cn:
    cur = cn.cursor()
    cur.execute(
        """
        SELECT t.name, COUNT(*)
        FROM sys.columns c
        JOIN sys.types t ON t.user_type_id=c.user_type_id
        JOIN sys.objects o ON o.object_id=c.object_id
        WHERE o.name='_Document704'
        GROUP BY t.name ORDER BY 2 DESC
        """
    )
    print(cur.fetchall())
    cur.execute(
        """
        SELECT c.name, t.name
        FROM sys.columns c
        JOIN sys.types t ON t.user_type_id=c.user_type_id
        JOIN sys.objects o ON o.object_id=c.object_id
        WHERE o.name='_Document704' AND (t.name LIKE '%date%' OR t.name LIKE '%time%')
        ORDER BY c.name
        """
    )
    print(cur.fetchall())
    # Also _Date_Time style
    cur.execute(
        """
        SELECT c.name, t.name
        FROM sys.columns c
        JOIN sys.types t ON t.user_type_id=c.user_type_id
        JOIN sys.objects o ON o.object_id=c.object_id
        WHERE o.name='_Document704' AND c.name LIKE '%Date%'
        ORDER BY c.name
        """
    )
    print("Date-named", cur.fetchall())
