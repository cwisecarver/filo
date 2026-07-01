"""End-to-end smoke test: the real django-libsql backend —
`libsql.db.backends.sqlite3`, built on libsql-client's DBAPI2 — talking to Filo
over a WebSocket. Exercises the Django ORM path (schema-editor DDL plus ORM
create/get), not just the raw driver. Invoked by Filo.IntegrationDjangoTest with
the server's ws:// URL as argv[1]. Prints DJANGO_OK on success.

Requires: django and django-libsql (e.g. `pip install django-libsql`).
"""
import sys

import django
from django.conf import settings

url = sys.argv[1]  # ws://127.0.0.1:PORT

# Standalone single-file Django app: the model lives in __main__, so __main__ is
# both the installed app and the model's app_label.
settings.configure(
    INSTALLED_APPS=["__main__"],
    DATABASES={
        "default": {
            "ENGINE": "libsql.db.backends.sqlite3",
            "NAME": url,
        }
    },
    USE_TZ=False,
)
django.setup()

from django.db import connection, models  # noqa: E402  (must follow django.setup)


class Company(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "__main__"


# DDL through the django-libsql schema editor.
with connection.schema_editor() as schema_editor:
    schema_editor.create_model(Company)

# ORM write + read round-tripped through Filo.
Company.objects.create(name="alice")
got = Company.objects.get(name="alice")
assert got.name == "alice", got.name

print("DJANGO_OK")
