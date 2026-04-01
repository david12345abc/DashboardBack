from django.db import models
from django.contrib.auth.hashers import make_password, check_password


class User(models.Model):
    class Role(models.TextChoices):
        USER1 = 'User1', 'User1 (Admin)'
        USER2 = 'User2', 'User2'
        USER3 = 'User3', 'User3'
        USER4 = 'User4', 'User4'
        USER5 = 'User5', 'User5'

    ROLE_PRIORITY = {
        Role.USER1: 1,
        Role.USER2: 2,
        Role.USER3: 3,
        Role.USER4: 4,
        Role.USER5: 5,
    }

    nickname = models.CharField(max_length=150, unique=True)
    password = models.CharField(max_length=256)
    role = models.CharField(max_length=10, choices=Role.choices, default=Role.USER5)
    department = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'users'

    def __str__(self):
        return f'{self.nickname} ({self.role})'

    def set_password(self, raw_password: str):
        self.password = make_password(raw_password)

    def verify_password(self, raw_password: str) -> bool:
        return check_password(raw_password, self.password)

    @property
    def is_admin(self) -> bool:
        return self.role == self.Role.USER1

    @property
    def priority(self) -> int:
        return self.ROLE_PRIORITY.get(self.role, 5)
