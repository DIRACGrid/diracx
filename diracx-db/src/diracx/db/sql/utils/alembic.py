"""Alembic Utilities."""
from __future__ import annotations

from alembic.autogenerate import comparators, renderers
from alembic.operations import MigrateOperation, Operations
from sqlalchemy import text

# Typing purposes
from sqlalchemy.sql.schema import MetaData


class Trigger:
    """Creates trigger definitions in a format accepted by Alembic."""

    def __init__(self, name: str, when: str, action: str, table: str, time: str, body: str):
        self.name : str = name
        self.when : str = when
        self.action : str = action
        self.table : str = table
        self.time : str = time
        self.body : str = body

    def create(self):
        return f"""\
            CREATE TRIGGER {self.name}
            {self.when} {self.action}
            ON {self.table}
            {self.time}
            BEGIN
            {self.body}
            END"""
    
    def drop(self):
        return f"DROP TRIGGER {self.name}"
    
    def __eq__(self, other: Trigger):
        return self.name == other.name
    
    def __repr__(self):
        return "Trigger(name=%r, when=%r, action=%r, table=%r, time=%r, body=%r)" \
                    % (self.name, self.when, self.action, self.table, self.time, self.body)

    def register_trigger(self, metadata: MetaData):
        """Add the trigger to database metadata."""
        metadata.info.setdefault("triggers", list()).append(self)

# Custom Alemic Operations
##########################
@Operations.register_operation("create_trigger")
class CreateTriggerOp(MigrateOperation):
    """Defines the operations to create triggers
    Executed by calling op.create_trigger.
    """

    def __init__(self, trigger: Trigger):
        self.trigger = trigger

    @classmethod
    def create_trigger(cls, operations, name: str, **kw):
        """Issue a "CREATE TRIGGER" instruction."""
        op = CreateTriggerOp(name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropTriggerOp(self.trigger)

@Operations.register_operation("drop_trigger")
class DropTriggerOp(MigrateOperation):
    """Defines the operations to drop triggers
    Executed by calling op.drop_trigger.
    """

    def __init__(self, trigger: Trigger):
        self.trigger = trigger

    @classmethod
    def drop_trigger(cls, operations, name: str, **kw):
        """Issue a "DROP TRIGGER" instruction."""
        op = DropTriggerOp(name)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return CreateTriggerOp(self.trigger)

@Operations.implementation_for(CreateTriggerOp)
def create_trigger(operations, operation):
    """Receives a CreteTriggerOp operation and executes its sql text for its creation."""
    sql_text = operation.trigger.create()
    operations.execute(sql_text)

@Operations.implementation_for(DropTriggerOp)
def drop_trigger(operations, operation):
    """Receives a DropTriggerOp operation and executes its sql text for its removal."""
    sql_text = operation.trigger.drop()
    operations.execute(sql_text)

#
# This function tells Alembic how to compare the state of the sqlalchemy matadata
#  to the one found in the currently deployed database
#
# Due to triggers being stored in the database metadata, and not inside the table, the comparator
#  executes at schema level.
@comparators.dispatch_for("schema")
def compare_triggers(autogen_context, operations, schemas):
    """Compares the current state of the Metadata with the previous one found in the DB."""
    all_conn_triggers = list()

    # Get the collection of Triggers objects stored inside the metadata context
    metadata_triggers = autogen_context.metadata.info.setdefault("triggers", list())

    for schema in schemas:
        # !!! This SQL Statement is MySQL specific !!!!
        statement = text(
            f"""SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_ORIENTATION, ACTION_TIMING, ACTION_STATEMENT \
                FROM information_schema.triggers \
                WHERE information_schema.triggers.trigger_schema \
                LIKE '{schema}';""")

        for row in autogen_context.connection.execute(statement):  
            trigger = Trigger(
                name=row["TRIGGER_NAME"],
                when=row['ACTION_TIMING'],
                action=row['EVENT_MANIPULATION'],
                table=schema,
                time="FOR EACH ROW",
                body=row['ACTION_STATEMENT']
            )

            all_conn_triggers.append(trigger)
    
    # For new triggers found in the metadata
    for trigger in metadata_triggers:
        # The trigger cannot be already in the db
        if trigger in all_conn_triggers:
            continue

        # The trigger is new, so produce a CreateTriggerOp directive
        operations.ops.append(
            CreateTriggerOp(trigger)
        )

    # For triggers that are in the database
    for trigger in all_conn_triggers:
        # The trigger cannot be in the metadata
        if trigger in metadata_triggers:
            continue

        # The trigger got removed, so produce a DropTriggerOp directives
        operations.ops.append(
            DropTriggerOp(trigger)
        )

#
# The renderer functions let alembic produce text that will be created inside the 
#  upgrade or downgrade action functions.
# 
# This renderers also save some information inside an special dictionary called mutable_structure
#  which let's us produce code inside the ".mako" template file 
@renderers.dispatch_for(CreateTriggerOp)
def render_create_sequence(autogen_context, op: CreateTriggerOp):
    """Almebic code renderer for CreateTrigger operations."""
    trigger = op.trigger
    
    ctx = autogen_context.opts['template_args']['mutable_structure']['triggers']
    
    if trigger not in ctx:
        ctx.append(trigger)
        
    # This part ends up being inside alembic's updagrade() or downgrade() functions
    return f"op.create_trigger({trigger.name})"


@renderers.dispatch_for(DropTriggerOp)
def render_drop_sequence(autogen_context, op: DropTriggerOp):
    """Almebic code renderer for DropTrigger operations."""
    trigger = op.trigger

    ctx = autogen_context.opts['template_args']['mutable_structure']['triggers']

    if trigger not in ctx:
        ctx.append(trigger)

    # This part ends up being inside alembic's updagrade() or downgrade() functions
    return f"op.drop_trigger({trigger.name})"