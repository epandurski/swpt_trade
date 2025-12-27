import re
from typing import Tuple, Optional
from enum import IntEnum
from flask import current_app, request, g
from flask_smorest import abort, Blueprint as BlueprintOrig
from swpt_pythonlib.utils import u64_to_i64

NOT_REQUIED = "false"
READ_ONLY_METHODS = ["GET", "HEAD", "OPTIONS"]


class Blueprint(BlueprintOrig):
    """A Blueprint subclass to use, that we may want to modify."""


class UserType(IntEnum):
    SUPERUSER = 1
    SUPERVISOR = 2
    CREDITOR = 3


class UserIdPatternMatcher:
    PATTERN_CONFIG_KEYS = {
        UserType.SUPERUSER: "APP_SUPERUSER_SUBJECT_REGEX",
        UserType.SUPERVISOR: "APP_SUPERVISOR_SUBJECT_REGEX",
        UserType.CREDITOR: "APP_CREDITOR_SUBJECT_REGEX",
    }

    def __init__(self):
        self._regex_patterns = {}

    def get_pattern(self, user_type: UserType) -> re.Pattern:
        pattern_config_key = self.PATTERN_CONFIG_KEYS[user_type]
        regex = current_app.config[pattern_config_key]
        regex_patterns = self._regex_patterns
        regex_pattern = regex_patterns.get(regex)
        if regex_pattern is None:
            regex_pattern = regex_patterns[regex] = re.compile(regex)

        return regex_pattern

    def match(self, user_id: str) -> Tuple[UserType, Optional[int]]:
        for user_type in UserType:
            pattern = self.get_pattern(user_type)
            m = pattern.match(user_id)
            if m:
                creditor_id = (
                    u64_to_i64(int(m.group(1)))
                    if user_type == UserType.CREDITOR
                    else None
                )
                return user_type, creditor_id

        abort(403)


user_id_pattern_matcher = UserIdPatternMatcher()


def parse_swpt_user_id_header() -> Tuple[UserType, Optional[int]]:
    user_id = request.headers.get("X-Swpt-User-Id")
    if user_id is None:
        user_type = UserType.SUPERUSER
        creditor_id = None
    else:
        user_type, creditor_id = user_id_pattern_matcher.match(user_id)

    g.superuser = user_type == UserType.SUPERUSER
    return user_type, creditor_id


def ensure_owner():
    user_type, creditor_id = parse_swpt_user_id_header()
    if (
            user_type == UserType.CREDITOR
            and creditor_id != current_app.config["OWNER_CREDITOR_ID"]
    ):
        abort(403)
