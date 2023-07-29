# -*- coding: utf-8 -*-

import typing as T
import time
import random
from datetime import datetime, timezone

from faker import Faker
import pynamodb_mate as pm
from boto_session_manager import BotoSesManager
from rich import print as rprint


fake = Faker()


def get_utc_now():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


class Movie(pm.Model):
    class Meta:
        table_name = "movie"
        region = "us-east-1"
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    id = pm.NumberAttribute(hash_key=True)
    title = pm.UnicodeAttribute()
    rating = pm.NumberAttribute()
    rating_updated_at = pm.UTCDateTimeAttribute()


bsm = BotoSesManager(profile_name="awshsh_app_dev_us_east_1")
with bsm.awscli():
    pm.Connection()

Movie.create_table(wait=True)


def trigger_event() -> T.Tuple[Movie, str]:
    id = random.randint(1, 1000)
    try:
        movie = Movie.get(id)
        movie.rating = random.randint(1, 100)
        movie.rating_updated_at = get_utc_now()
        movie.update(
            actions=[
                Movie.rating.set(movie.rating),
                Movie.rating_updated_at.set(movie.rating_updated_at),
            ]
        )
        return movie, "update"
    except Movie.DoesNotExist:
        movie = Movie(
            id=id,
            title=fake.sentence(),
            rating=random.randint(1, 100),
            rating_updated_at=get_utc_now(),
        )
        movie.save()
        return movie, "create"


i = 0
while 1:
    time.sleep(1)
    for _ in range(10):
        i += 1
        movie, action = trigger_event()
        print(f"{i} th: {action} {movie.attribute_values}")
