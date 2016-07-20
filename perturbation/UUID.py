"""

"""

import sqlalchemy.types
import uuid


class UUID(sqlalchemy.types.TypeDecorator):
    """

    """

    impl = sqlalchemy.types.LargeBinary(16)

    def process_bind_param(self, value, dialect):
        """

        :param value:
        :param dialect:

        :return:

        """

        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                try:
                    value = uuid.UUID(value)
                except(TypeError, ValueError):
                    value = uuid.UUID(bytes=value)

            return value.bytes
