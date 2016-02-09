"""

"""

import sqlalchemy.dialects.postgresql
import sqlalchemy.types
import uuid


class UUID(sqlalchemy.types.TypeDecorator):
    """

    """

    def python_type(self):
        """

        :return:

        """

        pass

    def process_literal_param(self, value, dialect):
        """

        :param value:
        :param dialect:

        :return:

        """

        pass

    impl = sqlalchemy.types.CHAR

    def load_dialect_impl(self, dialect):
        """

        :param dialect:

        :return:

        """

        if dialect.name == 'postgresql':
            return dialect.type_descriptor(sqlalchemy.dialects.postgresql.UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.types.CHAR(32))

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
                return "%.32x" % int(uuid.UUID(value))
            else:
                return "%.32x" % int(value)

    def process_result_value(self, value, dialect):
        """

        :param value:
        :param dialect:

        :return:

        """

        if value is None:
            return value
        else:
            return uuid.UUID(value)
