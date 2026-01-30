"""JSON snippets to be included in the OpenAPI specification file."""

DID = {
    "in": "path",
    "name": "debtorId",
    "required": True,
    "description": "The debtor's ID",
    "schema": {
        "type": "string",
        "pattern": "^[0-9A-Za-z_=-]{1,64}$",
    },
}

CID = {
    "in": "path",
    "name": "creditorId",
    "required": True,
    "description": "The creditor's ID",
    "schema": {
        "type": "string",
        "pattern": "^[0-9A-Za-z_=-]{1,64}$",
    },
}

LOCATION_HEADER = {
    "Location": {
        "description": "The URI of the entry.",
        "schema": {
            "type": "string",
            "format": "uri",
        },
    },
}

ERROR_CONTENT = {
    "application/json": {
        "schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "integer",
                    "format": "int32",
                    "description": "Error code",
                },
                "errors": {
                    "type": "object",
                    "description": "Errors",
                },
                "status": {
                    "type": "string",
                    "description": "Error name",
                },
                "message": {
                    "type": "string",
                    "description": "Error message",
                },
            },
        }
    }
}

OBJECT_EXISTS = {
    "description": "The same object already exists.",
    "headers": LOCATION_HEADER,
}

FORBIDDEN_OPERATION = {
    "description": "Forbidden operation.",
    "content": ERROR_CONTENT,
}

UPDATE_CONFLICT = {
    "description": "Conflicting update attempts.",
    "content": ERROR_CONTENT,
}

SCOPE_ACCESS_READONLY = [
    {"oauth2": ["access.readonly"]},
]

SCOPE_ACCESS_MODIFY = [
    {"oauth2": ["access"]},
]

SCOPE_ACTIVATE = [
    {"oauth2": ["activate"]},
]

SCOPE_DEACTIVATE = [
    {"oauth2": ["deactivate"]},
]

API_DESCRIPTION = """Admin API for the Swaptacular service that
performs circular trades between creditors.
"""

API_SPEC_OPTIONS = {
    "info": {
        "description": API_DESCRIPTION,
    },
    "servers": [
        {"url": "$API_ROOT"},
        {"url": "/"},
    ],
    "components": {
        "securitySchemes": {
            "oauth2": {
                "type": "oauth2",
                "description": (
                    "This API uses OAuth 2. [More info](https://oauth.net/2/)."
                ),
                "flows": {
                    "authorizationCode": {
                        "authorizationUrl": "$OAUTH2_AUTHORIZATION_URL",
                        "tokenUrl": "$OAUTH2_TOKEN_URL",
                        "refreshUrl": "$OAUTH2_REFRESH_URL",
                        "scopes": {
                            "access.readonly": "read-only access",
                            "access": "read-write access",
                        },
                    },
                    "clientCredentials": {
                        "tokenUrl": "$OAUTH2_TOKEN_URL",
                        "refreshUrl": "$OAUTH2_REFRESH_URL",
                        "scopes": {
                            "access.readonly": "read-only access",
                            "access": "read-write access",
                            "activate": "activate new creditors",
                            "deactivate": "deactivate existing creditors",
                        },
                    },
                },
            },
        },
    },
}
