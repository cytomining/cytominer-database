"""

"""

import sqlalchemy.dialects.postgresql
import sqlalchemy.types
import uuid


class UUID(sqlalchemy.types.TypeDecorator):
    """

    """

    impl = sqlalchemy.types.BINARY(16)

    python_type = uuid.UUID

    def load_dialect_impl(self, dialect):
        """

        :param dialect:

        :return:

        """

        if dialect.name == 'postgresql':
            return dialect.type_descriptor(sqlalchemy.dialects.postgresql.UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.types.BINARY(16))

    def process_bind_param(self, value, dialect):
        """

        :param value:
        :param dialect:

        :return:

        """

        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                try:
                    value = uuid.UUID(value)
                except(TypeError, ValueError):
                    value = uuid.UUID(bytes=value)

            return value.bytes

    def process_result_value(self, value, dialect):
        """

        :param value:
        :param dialect:

        :return:

        """

        if value is None:
            return value
        else:
            return uuid.UUID(bytes=value)
