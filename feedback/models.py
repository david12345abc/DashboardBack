from django.db import models


class FeedbackRequest(models.Model):
    class Topic(models.TextChoices):
        QUESTION = 'question', 'Вопрос по показателю'
        BUG = 'bug', 'Сообщить об ошибке'
        DATA = 'data', 'Проблема с данными'
        FEATURE = 'feature', 'Предложение по улучшению'

    class Status(models.TextChoices):
        NEW = 'new', 'Новое'
        SENT = 'sent', 'Отправлено'
        FAILED = 'failed', 'Ошибка отправки'
        COMPLETED = 'completed', 'Выполнено'
        REJECTED = 'rejected', 'Отклонено'

    user = models.ForeignKey(
        'User.User',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='feedback_requests',
    )
    topic = models.CharField(max_length=32, choices=Topic.choices, default=Topic.QUESTION)
    department = models.CharField(max_length=255, blank=True)
    related_department = models.CharField(max_length=255, blank=True)
    dashboard = models.CharField(max_length=255, blank=True)
    period = models.CharField(max_length=120, blank=True)
    description = models.TextField()
    contact = models.CharField(max_length=255, blank=True)
    attachment_names = models.JSONField(default=list, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NEW)
    email_error = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'feedback_requests'
        ordering = ['-created_at']

    def __str__(self):
        return f'{self.get_topic_display()} ({self.department or "без подразделения"})'

# Create your models here.
