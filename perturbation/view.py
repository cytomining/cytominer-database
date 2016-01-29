import sqlalchemy
import sqlalchemy.schema
import sqlalchemy.ext.compiler


class CreateView(sqlalchemy.schema.DDLElement):
    def __init__(self, name, selectable):
        self.name = name

        self.selectable = selectable


@sqlalchemy.ext.compiler.compiles(CreateView)
def compiles(element, compiler, **kwargs):
    name = element.name

    selected = compiler.sql_compiler.process(element.selectable, literal_binds=True)

    return 'CREATE VIEW IF NOT EXISTS {0:s} AS {1:s}'.format(name, selected)


def create_view(metadata, name, selectable):
    table = sqlalchemy.table(name)

    for c in selectable.c:
        c._make_proxy(table)

    CreateView(name, selectable).execute_at('after-create', metadata)

    sqlalchemy.DDL('DROP VIEW IF EXISTS {}'.format(name)).execute_at('before_drop', metadata)

    return table
