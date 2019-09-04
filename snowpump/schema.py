from marshmallow import Schema, fields, EXCLUDE


class HostSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    name = fields.String(attribute="host_name", required=True)
    hostgroups = fields.List(fields.String(), missing=[])
